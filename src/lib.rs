use std::{
    io::Write,
    ops::{Deref, DerefMut},
    ptr::null_mut,
    sync::atomic::{AtomicUsize, Ordering::*},
};

pub struct Ringo<const N: usize> {
    allocations: AtomicUsize,
    buffers: *mut *mut u8,
    size: usize,
    head: AtomicUsize,
    tail: AtomicUsize,
}

impl<const N: usize> Ringo<N> {
    pub fn new(size: usize) -> &'static Self {
        assert!(size != 0 && N > 1);

        let mut buffers = vec![null_mut(); N].into_boxed_slice();

        for cell in &mut buffers {
            let allocation: Box<[u8]> = Vec::with_capacity(size).into_boxed_slice();
            let pointer = Box::leak(allocation).as_mut_ptr();
            *cell = pointer;
        }
        let buffers = Box::leak(buffers).as_mut_ptr();
        let head = AtomicUsize::new(1);
        let tail = AtomicUsize::new(0);
        let allocations = AtomicUsize::new(0);

        let this = Box::new(Self {
            allocations,
            buffers,
            size,
            tail,
            head,
        });
        Box::leak(this)
    }

    pub fn alloc(&'static self) -> Option<Buffer<N>> {
        (self.allocations.load(Relaxed) != N).then_some(())?;

        let mut head = self.head.load(Acquire);
        loop {
            let new = (head + 1) % N;
            let result = self.head.compare_exchange(head, new, Release, Acquire);
            if let Err(modified) = result {
                head = modified;
                continue;
            }
            let pointer = unsafe { *self.buffers.add(head) };
            self.allocations.fetch_add(1, Relaxed);
            break Some(Buffer::new(self, pointer));
        }
    }
}

pub struct Buffer<const N: usize> {
    ringo: &'static Ringo<N>,
    inner: &'static mut [u8],
}

impl<const N: usize> Write for Buffer<N> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

impl<const N: usize> Deref for Buffer<N> {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        self.inner
    }
}

impl<const N: usize> DerefMut for Buffer<N> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner
    }
}

impl<const N: usize> Buffer<N> {
    fn new(ringo: &'static Ringo<N>, pointer: *mut u8) -> Self {
        let inner = unsafe { std::slice::from_raw_parts_mut(pointer, ringo.size) };
        Self { ringo, inner }
    }
}

impl<const N: usize> Drop for Buffer<N> {
    fn drop(&mut self) {
        let mut tail = self.ringo.tail.load(Acquire);
        loop {
            let new = (tail + 1) % N;
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
        self.ringo.allocations.fetch_sub(1, Relaxed);
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
