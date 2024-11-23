use std::{
    io::{self, Write},
    ops::{Deref, DerefMut},
    sync::atomic::{AtomicU32, Ordering::*},
};

const MAXALLOC: usize = (u32::MAX >> 1) as usize;

pub struct Ringo {
    buffer: *mut u32,
    end: *const u32,
    head: *mut u32,
    capacity: usize,
}

const HEADER: usize = size_of::<u32>();

impl Ringo {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > HEADER);
        let capacity = capacity.next_power_of_two() / HEADER;
        let buffer = vec![MAXALLOC as u32; capacity];
        let buffer = Box::leak(buffer.into_boxed_slice()).as_mut_ptr();
        let head = buffer;
        let end = unsafe { buffer.add(capacity - 1) };
        Self {
            buffer,
            end,
            head,
            capacity,
        }
    }

    pub fn writer(&mut self, min: usize) -> Option<BufferWriter<'_>> {
        let inner = self.alloc(min)?;
        let header = inner as *const AtomicU32;
        let inner = unsafe { inner.add(1) } as *mut u8;
        let length = unsafe { &*header }.load(Acquire) >> 1;

        Some(BufferWriter {
            header,
            inner,
            initialized: 0,
            length,
            ringo: self,
        })
    }

    fn try_extend(&mut self, header: *const AtomicU32, extra: usize) -> io::Result<u32> {
        let allocation = self
            .alloc(extra)
            .ok_or_else(|| io::Error::other("ring buffer is full"))?;
        let length = unsafe { &*(allocation as *const AtomicU32) }.load(Acquire) >> 1;
        unsafe { &*header }.fetch_add(length + 1, Release);
        Ok(length + 1)
    }

    pub fn fixed(&mut self, min: usize) -> Option<BufferMut> {
        let allocation = self.alloc(min)?;
        let header = allocation as *const AtomicU32;
        let inner = unsafe { allocation.add(1) } as *mut u8;
        let len = unsafe { &*header }.load(Acquire) >> 1;
        unsafe { &*header }.fetch_or(1 << 31, Release);
        let inner = unsafe { std::slice::from_raw_parts_mut(inner, len as usize) };
        Some(BufferMut {
            header,
            inner,
            initialized: 0,
        })
    }

    fn alloc(&mut self, min: usize) -> Option<*mut u32> {
        (min <= MAXALLOC * HEADER).then_some(())?;
        (self.capacity > min + HEADER).then_some(())?;
        let min = (min / HEADER) as u32 + (min % HEADER != 0) as u32;

        let header = self.head as *const AtomicU32;
        let value = unsafe { &*header }.load(Acquire);
        if (value >> 31) != 0 {
            return None;
        }
        let mut length = value >> 1;
        let mut wrapped = false;
        while length < min {
            let mut header = unsafe { self.head.add(length as usize + HEADER).cast_const() };
            if header >= self.end {
                header = self.buffer;
                wrapped = true;
                length = 0;
            }
            let header = header as *const AtomicU32;
            let value = unsafe { &*header }.load(Acquire);
            if (value >> 31) != 0 {
                return None;
            }
            length += value >> 1;
        }
        let gap = length - min;
        length = length.min(min);
        if wrapped {
            let len = unsafe { self.end.offset_from(self.head) } as u32;
            unsafe { &*header }.store(len, Release);
            self.head = self.buffer;
        }
        let result = self.head;
        self.head = unsafe { self.head.add((length - gap) as usize) };
        if gap != 0 {
            let header = self.head as *const AtomicU32;
            unsafe { &*header }.store(gap, Release);
        }
        Some(result)
    }
}

pub struct BufferWriter<'a> {
    header: *const AtomicU32,
    inner: *mut u8,
    initialized: u32,
    length: u32,
    ringo: &'a mut Ringo,
}

impl<'a> BufferWriter<'a> {
    pub fn finish(self) -> BufferMut {
        unsafe { &*self.header }.store(1 << 31 | self.length, Release);
        let inner =
            unsafe { std::slice::from_raw_parts_mut(self.inner, self.initialized as usize) };
        BufferMut {
            header: self.header,
            inner,
            initialized: 0,
        }
    }
}

impl<'a> Write for BufferWriter<'a> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let required = unsafe { self.inner.add(self.initialized as usize + buf.len()) };
        let end = unsafe { self.inner.add(self.length as usize) };
        if required > end {
            if self.length as usize + buf.len() > MAXALLOC {
                io::Error::other("max allocation exceeded");
            }
            let extra = unsafe { required.offset_from(end) } as usize;
            self.length += self.ringo.try_extend(self.header, extra)?;
        }
        unsafe { self.inner.copy_from_nonoverlapping(buf.as_ptr(), buf.len()) };
        self.initialized += buf.len() as u32;
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub struct BufferMut {
    header: *const AtomicU32,
    inner: &'static mut [u8],
    initialized: u32,
}

impl Write for BufferMut {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let size = self.inner.write(buf)?;
        self.initialized += size as u32;
        Ok(size)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub struct Buffer {
    header: *const AtomicU32,
    inner: &'static [u8],
    rc: &'static AtomicU32,
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
            header: self.header,
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
        unsafe { &*self.header }.fetch_or(u32::MAX >> 1, Release);
        // SAFETY: checked above that we are the last reference holder,
        // which makes it safe to reclaim the storage for AtomicU32
        let _ = unsafe { Box::from_raw(self.rc.as_ptr() as *mut AtomicU32) };
    }
}

impl Drop for BufferMut {
    fn drop(&mut self) {
        unsafe { &*self.header }.fetch_or(u32::MAX >> 1, Release);
    }
}

impl Deref for BufferMut {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        unsafe { self.inner.get_unchecked(0..self.initialized as usize) }
    }
}

impl DerefMut for BufferMut {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.inner.get_unchecked_mut(0..self.initialized as usize) }
    }
}

impl BufferMut {
    pub fn init(&mut self) {
        self.inner.fill(0);
        self.initialized = self.inner.len() as u32;
    }

    pub fn freeze(self) -> Buffer {
        let rc = Box::leak(Box::new(AtomicU32::new(1)));
        let inner = unsafe { std::slice::from_raw_parts(self.inner.as_ptr(), self.inner.len()) };
        Buffer {
            inner,
            rc,
            header: self.header,
        }
    }
}
//#[cfg(test)]
//mod tests {
//    use crate::*;
//    const BUFSIZE: usize = 1024;
//    const BUFCOUNT: usize = 8;
//
//    #[test]
//    fn test_alloc() {
//        let ringo = Ringo::<BUFCOUNT>::new(BUFSIZE);
//
//        let buffer = ringo.alloc().unwrap();
//        assert_eq!(buffer.len(), BUFSIZE);
//    }
//
//    #[test]
//    fn test_alloc_fail() {
//        let ringo = Ringo::<BUFCOUNT>::new(BUFSIZE);
//        let mut buffers = Vec::with_capacity(BUFCOUNT);
//        for _ in 0..BUFCOUNT {
//            let buffer = ringo.alloc().unwrap();
//            buffers.push(buffer);
//        }
//        let buffer = ringo.alloc();
//        assert!(buffer.is_none())
//    }
//
//    #[test]
//    fn test_realloc() {
//        let ringo = Ringo::<BUFCOUNT>::new(BUFSIZE);
//        const DROP_ORDER: [usize; 8] = [1, 0, 2, 5, 3, 7, 4, 6];
//        let mut buffers = Vec::with_capacity(BUFCOUNT);
//        for _ in 0..BUFCOUNT {
//            let buffer = ringo.alloc().unwrap();
//            buffers.push(Some(buffer));
//        }
//        let mut addrs = Vec::with_capacity(BUFCOUNT);
//        for i in DROP_ORDER {
//            addrs.push(buffers[i].take().unwrap().as_ptr());
//        }
//        for addr in addrs {
//            let buffer = ringo.alloc().unwrap();
//            assert_eq!(addr, buffer.as_ptr())
//        }
//    }
//}
