use std::{
    io::{self, Write},
    ops::{Deref, DerefMut},
    sync::atomic::{AtomicU32, Ordering::*},
};

const U32SIZE: usize = size_of::<u32>();
const MAXALLOC: u32 = u32::MAX >> 1;
const MAXALLOCBYTES: usize = (u32::MAX >> 1) as usize * U32SIZE;
const MINALLOC: u32 = 8;
const MINALLOCBYTES: usize = MINALLOC as usize * U32SIZE;

pub struct Ringal {
    start: *mut u32,
    end: *const u32,
    head: *mut u32,
}

type HeaderMut = Header<*mut AtomicU32>;
type HeaderRo = Header<*const AtomicU32>;

struct Header<P>(P);

impl Ringal {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > U32SIZE + MINALLOCBYTES);
        let capacity = capacity.next_power_of_two() / U32SIZE;
        let mut buffer = Vec::with_capacity(capacity);
        let cap = capacity as u32;
        for i in 0..cap {
            // TODO(perf): figure out how init this memory faster
            buffer.push(MAXALLOC.min(cap - i - 1) << 1);
        }

        let start = Box::leak(buffer.into_boxed_slice()).as_mut_ptr();
        let head = start;
        let end = unsafe { start.add(capacity - 1) };
        Self { start, end, head }
    }

    fn alloc(&mut self, min: usize) -> Option<HeaderMut> {
        let min = (min / U32SIZE) as u32 + (min % U32SIZE != 0) as u32;
        (MINALLOC..MAXALLOC).contains(&min).then_some(())?;

        self.advance(min)
    }

    pub fn writer(&mut self, min: usize) -> Option<BufferWriter<'_>> {
        let header = self.alloc(min)?;
        let inner = header.buffer();
        let capacity = header.capacity() as usize * U32SIZE;

        Some(BufferWriter {
            header,
            inner,
            initialized: 0,
            capacity,
            ringo: self,
        })
    }

    /// # Safety
    /// returns uninitialized memory of requested size rounded up to alignment
    /// call `fill` on returned buffer if initializition is required, otherwise
    /// don't read data before writing something to buffer
    pub unsafe fn fixed(&mut self, min: usize) -> Option<BufferMut> {
        let header = self.alloc(min)?;
        let capacity = header.capacity() as usize;
        let inner = header.buffer();
        header.set();
        let buffer = BufferMut {
            header: header.into(),
            inner: unsafe { std::slice::from_raw_parts_mut(inner, capacity) },
        };
        Some(buffer)
    }

    fn extend(&mut self, header: &HeaderMut, extra: usize) -> io::Result<()> {
        let extra = self
            .alloc(extra - U32SIZE)
            .ok_or_else(|| io::Error::other("ring buffer is full"))?;
        let capacity = extra.capacity() + 1 + header.capacity();
        header.store(capacity);
        Ok(())
    }

    fn advance(&mut self, capacity: u32) -> Option<HeaderMut> {
        let mut accumulated = 0;
        let mut current = self.head;
        let mut wrapped = false;
        loop {
            let header = Header::new(current);
            header.available().then_some(())?;

            let size = header.capacity();
            accumulated += size;
            if accumulated >= capacity {
                break;
            }

            let next = unsafe { current.add(size as usize + U32SIZE) };
            accumulated += 1;

            current = if next.cast_const() >= self.end {
                accumulated = 0;
                wrapped = true;
                self.start
            } else {
                next
            };
            (self.head != current).then_some(())?;
        }
        if wrapped {
            self.head = self.start
        };
        let header = Header::new(self.head);
        let cap = if accumulated - capacity <= MINALLOC {
            accumulated
        } else {
            capacity
        };
        header.store(cap);
        let next = unsafe { self.head.add(cap as usize + 1) };
        if next.cast_const() >= self.end {
            self.head = self.start;
        } else {
            self.head = next;
            if accumulated - capacity > MINALLOC {
                let header = Header::new(self.head);
                let distance = unsafe { self.end.offset_from(self.head) } as u32;
                header.store((accumulated - capacity).min(distance));
                header.unset();
            }
        }

        Some(header)
    }
}

impl Clone for HeaderRo {
    fn clone(&self) -> Self {
        Self(self.0)
    }
}

impl HeaderMut {
    fn new(ptr: *mut u32) -> Self {
        Self(ptr as *mut AtomicU32)
    }

    fn set(&self) {
        unsafe { &*self.0 }.fetch_or(1, Release);
    }

    fn unset(&self) {
        unsafe { &*self.0 }.fetch_and(u32::MAX << 1, Release);
    }

    fn store(&self, size: u32) {
        unsafe { &*self.0 }.store(size << 1, Release);
    }

    fn capacity(&self) -> u32 {
        unsafe { &*self.0 }.load(Acquire) >> 1
    }

    fn available(&self) -> bool {
        (unsafe { &*self.0 }.load(Acquire) & 1) == 0
    }

    fn buffer(&self) -> *mut u8 {
        unsafe { self.0.add(1) as *mut u8 }
    }
}

impl HeaderRo {
    fn unset(&self) {
        unsafe { &*self.0 }.fetch_and(u32::MAX << 1, Release);
    }
}

pub struct BufferWriter<'a> {
    inner: *mut u8,
    header: Header<*mut AtomicU32>,
    initialized: usize,
    capacity: usize,
    ringo: &'a mut Ringal,
}

impl<'a> BufferWriter<'a> {
    pub fn finish(self) -> BufferMut {
        let inner = unsafe { std::slice::from_raw_parts_mut(self.inner, self.initialized) };
        self.header.set();
        BufferMut {
            header: self.header.into(),
            inner,
        }
    }
}

impl<'a> Write for BufferWriter<'a> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if self.initialized == MAXALLOCBYTES {
            io::Error::other("max allocation size reached");
        }
        let len = buf.len().min(MAXALLOCBYTES - self.initialized);

        let required = self.initialized + len;
        if required > self.capacity {
            let extra = (required - self.capacity).max(MINALLOCBYTES);
            self.ringo.extend(&self.header, extra)?;
            self.capacity = self.header.capacity() as usize * U32SIZE;
        }
        unsafe {
            self.inner
                .add(self.initialized)
                .copy_from_nonoverlapping(buf.as_ptr(), len)
        };
        self.initialized += len;
        Ok(len)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub struct BufferMut {
    header: HeaderRo,
    inner: &'static mut [u8],
}

pub struct Buffer {
    header: HeaderRo,
    inner: &'static [u8],
    rc: &'static AtomicU32,
}

impl From<HeaderMut> for HeaderRo {
    fn from(value: HeaderMut) -> Self {
        Self(value.0)
    }
}

impl Deref for Buffer {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        self.inner
    }
}

impl Clone for Buffer {
    fn clone(&self) -> Self {
        self.rc.fetch_add(1, Release);
        Self {
            inner: self.inner,
            rc: self.rc,
            header: self.header.clone(),
        }
    }
}

impl Drop for Buffer {
    fn drop(&mut self) {
        let mut count = self.rc.load(Acquire);
        loop {
            if count == 1 {
                break;
            }
            if let Err(c) = self.rc.compare_exchange(count, count - 1, Release, Acquire) {
                count = c;
            } else {
                return;
            }
        }
        self.header.unset();
        // SAFETY: checked above that we are the last reference holder,
        // which makes it safe to reclaim the storage for AtomicU32
        let _ = unsafe { Box::from_raw(self.rc.as_ptr() as *mut AtomicU32) };
    }
}

impl Drop for BufferMut {
    fn drop(&mut self) {
        self.header.unset();
    }
}

impl Deref for BufferMut {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        self.inner
    }
}

impl DerefMut for BufferMut {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner
    }
}

impl BufferMut {
    pub fn init(&mut self) {
        self.inner.fill(0);
    }

    pub fn freeze(self) -> Buffer {
        let rc = Box::leak(Box::new(AtomicU32::new(1)));
        let inner = unsafe { std::slice::from_raw_parts(self.inner.as_ptr(), self.inner.len()) };
        let ro = Buffer {
            inner,
            rc,
            header: self.header.clone(),
        };
        // don't run Drop on BufferMut, Buffer is now responsible for cleanup
        std::mem::forget(self);
        ro
    }
}
#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use crate::*;
    const BUFSIZE: usize = 1024;
    const CHUNKSIZE: usize = MINALLOCBYTES;
    const TESTMSG: &[u8] = b"TEST MESSAGE";
    const LONGMSG: &[u8] = b"THIS IS A VERY LONG MESSAGE! THIS IS A VERY LONG MESSAGE! THIS IS A VERY LONG MESSAGE! THIS IS A VERY LONG MESSAGE! THIS IS A VERY LONG MESSAGE!";
    const LONGMSGLEN: usize = LONGMSG.len();

    #[test]
    fn test_alloc() {
        let mut ringal = Ringal::new(BUFSIZE);
        let header1 = ringal.alloc(CHUNKSIZE).unwrap();
        assert!(header1.available());
        assert_eq!(header1.capacity(), (CHUNKSIZE / U32SIZE) as u32);
        let header2 = ringal.alloc(CHUNKSIZE).unwrap();
        assert_eq!(header2.0, unsafe { header1.0.add(CHUNKSIZE / U32SIZE + 1) });
    }

    #[test]
    fn test_writer() {
        let mut ringal = Ringal::new(BUFSIZE);

        let mut buffer = ringal.writer(CHUNKSIZE).unwrap();
        assert!(buffer.write(TESTMSG).is_ok());
        let buffer = buffer.finish();
        assert_eq!(buffer.as_ref(), TESTMSG);
        let buffer = buffer.freeze();
        assert_eq!(buffer.as_ref(), TESTMSG);
    }

    #[test]
    fn test_extendable_writer() {
        let mut ringal = Ringal::new(BUFSIZE);

        let mut buffer = ringal.writer(CHUNKSIZE).unwrap();
        let result = buffer.write(LONGMSG);
        assert!(result.is_ok());
        let buffer = buffer.finish();
        assert_eq!(buffer.as_ref(), LONGMSG);
        let header = ringal.alloc(CHUNKSIZE).unwrap();
        let offset = unsafe { buffer.as_ptr().add(LONGMSGLEN) };
        assert_eq!(offset, header.0 as *const u8);
    }

    #[test]
    fn test_alloc_fail() {
        let mut ringal = Ringal::new(BUFSIZE);
        let count = BUFSIZE / (CHUNKSIZE + U32SIZE);
        let mut buffers = Vec::with_capacity(count);
        for _ in 0..count {
            let buffer = unsafe { ringal.fixed(CHUNKSIZE) }.unwrap();
            buffers.push(buffer);
        }
        let buffer = ringal.alloc(CHUNKSIZE);
        assert!(buffer.is_none());
    }

    #[test]
    fn test_realloc() {
        let mut ringal = Ringal::new(BUFSIZE);
        let buffer1 = unsafe { ringal.fixed(BUFSIZE / 2 - U32SIZE * 2) }.unwrap();
        let _buffer2 = unsafe { ringal.fixed(BUFSIZE / 2 - U32SIZE * 2) }.unwrap();
        assert!(ringal.alloc(CHUNKSIZE).is_none());
        drop(buffer1);
        assert!(ringal.alloc(CHUNKSIZE).is_some());
    }

    #[test]
    fn test_continuous_realloc() {
        let mut ringal = Ringal::new(BUFSIZE);
        let iterations = BUFSIZE / (CHUNKSIZE + U32SIZE) * 10;
        let mut buffers = VecDeque::with_capacity(2);
        buffers.push_back(unsafe { ringal.fixed(MINALLOCBYTES) }.unwrap());
        for i in MINALLOCBYTES..MINALLOCBYTES * 2 {
            for _ in 0..iterations {
                buffers.push_back(unsafe { ringal.fixed(i) }.unwrap());
                buffers.pop_front();
            }
        }
    }
}
