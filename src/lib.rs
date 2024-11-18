use std::{
    io::Write,
    ops::{Deref, DerefMut, RangeBounds},
    ptr::null_mut,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering::*},
        Arc,
    },
};

struct AtomicSliceMut {
    initialized: usize,
    refs: Arc<()>,
    buffer: *mut u8,
}

impl AtomicSlice {
    fn new(buffer: *mut u8) -> Self {
        let refs = AtomicU8::new(0);
        Self { refs, buffer }
    }
}

pub struct Ringo<const N: usize> {
    buffers: *mut AtomicSlice,
    size: usize,
    index: AtomicUsize,
}

impl<const N: usize> Ringo<N> {
    pub fn new(size: usize) -> &'static Self {
        assert!(size != 0 && (1..u32::MAX as usize).contains(&N));

        let mut buffers = Vec::with_capacity(N);

        let allocation: Box<[u8]> = Vec::with_capacity(N * size).into_boxed_slice();
        let pointer = Box::leak(allocation).as_mut_ptr();
        for i in 0..N {
            buffers.push(AtomicSlice::new(unsafe { pointer.add(i * size) }));
        }
        let buffers = Box::leak(buffers.into_boxed_slice()).as_mut_ptr();
        let index = AtomicUsize::new(0);

        let this = Box::new(Self {
            buffers,
            size,
            index,
        });
        Box::leak(this)
    }

    pub fn alloc(&'static self) -> Option<Buffer<N>> {
        let (mut index, mut allocations) = index_and_allocations(self.atomic.load(Acquire));
        loop {
            (allocations < N).then_some(())?;
            let old = ((index as u64) << 32) + (allocations as u64);
            let new = (((index + 1) as u64) << 32) + ((allocations + 1) as u64);

            let result = self.atomic.compare_exchange(old, new, Release, Acquire);
            if let Err(modified) = result {
                (index, allocations) = index_and_allocations(modified);
                continue;
            }
            let pointer = unsafe { *self.buffers.add(index) };
            break Some(Buffer::new(self, pointer));
        }
    }

    pub fn extend(&'static self, buffer: LongBuffer<N>) -> Result<LongBuffer<N>, LongBuffer<N>> {
        let (index, allocations) = index_and_allocations(self.atomic.load(Acquire));
        if allocations < N {
            return Err(buffer);
        }
        unsafe {
            match buffer {
                LongBuffer::Continuous(mut b) => {
                    let ends =
                        b.as_ptr().add(b.len()) == (*self.buffers.add(N - 1)).add(self.size - 1);
                    if index == 0 && ends {
                        let old = allocations as u64;
                        let new = (1 << 32) + ((allocations + 1) as u64);
                        if self
                            .atomic
                            .compare_exchange(old, new, Release, Acquire)
                            .is_err()
                        {
                            return Err(LongBuffer::Continuous(b));
                        }
                        let pointer = *self.buffers;
                        let start = Buffer::new(self, pointer);
                        return Ok(LongBuffer::WrapAround { end: b, start });
                    }
                    let next = *self.buffers.add(index);
                    if b.as_ptr().add(self.size) == next {
                        return Err(LongBuffer::Continuous(b));
                    }
                    let old = ((index as u64) << 32) + (allocations as u64);
                    let new = (((index + 1) as u64) << 32) + ((allocations + 1) as u64);
                    if self
                        .atomic
                        .compare_exchange(old, new, Release, Acquire)
                        .is_err()
                    {
                        return Err(LongBuffer::Continuous(b));
                    }
                    b.inner = std::slice::from_raw_parts_mut(b.as_mut_ptr(), b.len() + self.size);
                    return Ok(LongBuffer::Continuous(b));
                }
                LongBuffer::WrapAround { end, mut start } => {
                    let next = *self.buffers.add(index);
                    if start.as_ptr().add(self.size) == next {
                        return Err(LongBuffer::WrapAround { end, start });
                    }
                    let old = ((index as u64) << 32) + (allocations as u64);
                    let new = (((index + 1) as u64) << 32) + ((allocations + 1) as u64);
                    if self
                        .atomic
                        .compare_exchange(old, new, Release, Acquire)
                        .is_err()
                    {
                        return Err(LongBuffer::WrapAround { end, start });
                    }
                    start.inner =
                        std::slice::from_raw_parts_mut(start.as_mut_ptr(), start.len() + self.size);
                    return Ok(LongBuffer::WrapAround { end, start });
                }
            }
        }
    }
}

pub enum LongBuffer<const N: usize> {
    Continuous(Buffer<N>),
    WrapAround { end: Buffer<N>, start: Buffer<N> },
}

impl<const N: usize> From<Buffer<N>> for LongBuffer<N> {
    fn from(value: Buffer<N>) -> Self {
        Self::Continuous(value)
    }
}

impl<const N: usize> Write for Buffer<N> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let range = self.initialized..self.ringo.size;
        let mut uninit = unsafe { self.get_unchecked_mut(range) };
        let written = uninit.write(buf)?;
        self.initialized = written;
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

impl<const N: usize> Deref for Buffer<N> {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        unsafe { self.inner.get_unchecked(0..self.initialized) }
    }
}

impl<const N: usize> DerefMut for Buffer<N> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.inner.get_unchecked_mut(0..self.initialized) }
    }
}

impl<const N: usize> Buffer<N> {
    fn new(ringo: &'static Ringo<N>, pointer: *mut u8) -> Self {
        let inner = unsafe { std::slice::from_raw_parts_mut(pointer, ringo.size) };
        let initialized = 0;
        Self {
            ringo,
            inner,
            initialized,
        }
    }

    pub fn uninitialized(&self) -> usize {
        self.inner.len() - self.initialized
    }
}

impl<const N: usize> Drop for Buffer<N> {
    fn drop(&mut self) {
        let (index, allocations) = index_and_allocations(self.ringo.atomic.load(Acquire));
        loop {
            let old = ((index as u64) << 32) + (allocations as u64);
            let new = ((index as u64) << 32) + ((allocations - 1) as u64);

            let result = self
                .ringo
                .tail
                .compare_exchange(tail, new, Release, Acquire);
            if let Err(modified) = result {
                tail = modified;
            } else {
                tail = new;
                break;
            }
        }
        self.ringo.allocations.fetch_sub(1, Acquire);
        unsafe { *self.ringo.buffers.add(tail) = self.inner.as_mut_ptr() };
    }
}

#[cfg(test)]
mod tests {
    use crate::*;
    const BUFSIZE: usize = 1024;
    const BUFCOUNT: usize = 8;

    #[test]
    fn test_alloc() {
        let ringo = Ringo::<BUFCOUNT>::new(BUFSIZE);

        let buffer = ringo.alloc().unwrap();
        assert_eq!(buffer.len(), BUFSIZE);
    }

    #[test]
    fn test_alloc_fail() {
        let ringo = Ringo::<BUFCOUNT>::new(BUFSIZE);
        let mut buffers = Vec::with_capacity(BUFCOUNT);
        for _ in 0..BUFCOUNT {
            let buffer = ringo.alloc().unwrap();
            buffers.push(buffer);
        }
        let buffer = ringo.alloc();
        assert!(buffer.is_none())
    }

    #[test]
    fn test_realloc() {
        let ringo = Ringo::<BUFCOUNT>::new(BUFSIZE);
        const DROP_ORDER: [usize; 8] = [1, 0, 2, 5, 3, 7, 4, 6];
        let mut buffers = Vec::with_capacity(BUFCOUNT);
        for _ in 0..BUFCOUNT {
            let buffer = ringo.alloc().unwrap();
            buffers.push(Some(buffer));
        }
        let mut addrs = Vec::with_capacity(BUFCOUNT);
        for i in DROP_ORDER {
            addrs.push(buffers[i].take().unwrap().as_ptr());
        }
        for addr in addrs {
            let buffer = ringo.alloc().unwrap();
            assert_eq!(addr, buffer.as_ptr())
        }
    }
}
