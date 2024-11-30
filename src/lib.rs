//! Allocator based on ring buffer. All allocations are done from underlying primary buffer, in a
//! cicular manner, i.e. the allocator wraps around once it reaches the end. Allocation is a very
//! cheap/fast operation, as it usually involves only adjusting a few pointers. The provided buffer
//! can safely sent to other threads or even cloned (after making it immutable), once the buffer is
//! dropped its backing store becomes available for new allocations. Primary use case is to
//! allocate scratch memory from ring buffer, fill it with data and hand it off to reader (on the
//! same thread or other threads), which promptly discards the buffer after it get's what it needs
//! from it, so that no one holds on to memory buffers for too long, otherwise the allocator will
//! just run out of allocatable memory. Current implementation uses head pointer which performs
//! quick scan of primary buffer for usable segments and hands them off to caller, but if it
//! encouneters busy segment it will terminate the scan, so it's possible to jam the allocator by
//! not releasing a single small segment.

use std::io;

use buffer::{BufferMut, BufferWriter};
use header::HeaderMut;

const U32SIZE: usize = size_of::<u32>();
const MAXALLOC: u32 = u32::MAX >> 1;
pub const MAXALLOCBYTES: usize = (u32::MAX >> 1) as usize * U32SIZE;
const MINALLOC: u32 = 8;
pub const MINALLOCBYTES: usize = MINALLOC as usize * U32SIZE;

/// Ring Allocator, unclonable type, which should be owned by a single writer thread, which can
/// then dispatch allocated buffers to other threads safely. See crate level documentation
pub struct Ringal {
    /// Start of the primary buffer
    start: *mut u32,
    /// End of the primary buffer
    end: *const u32,
    /// current location of head pointer, next allocation
    /// request will be attempted from this address
    head: *mut u32,
}

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
            ringal: self,
        })
    }

    /// # Safety
    /// returns uninitialized memory of requested size rounded up to MINALLOCBYTES
    /// call `fill` on returned buffer if initializition is required, otherwise
    /// don't read data before writing something to buffer
    pub unsafe fn fixed_uninit(&mut self, min: usize) -> Option<BufferMut> {
        let header = self.alloc(min)?;
        let inner = header.buffer();
        header.set();
        let buffer = BufferMut {
            header: header.into(),
            inner: unsafe { std::slice::from_raw_parts_mut(inner, min) },
        };
        Some(buffer)
    }

    pub fn fixed(&mut self, min: usize) -> Option<BufferMut> {
        let mut buffer = unsafe { self.fixed_uninit(min) }?;
        buffer.fill(0);
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
            let header = HeaderMut::new(current);
            header.available().then_some(())?;

            let size = header.capacity();
            accumulated += size;
            if accumulated >= capacity {
                break;
            }

            let next = unsafe { current.add(size as usize + 1) };
            accumulated += 1;

            current = if next.cast_const() >= self.end {
                accumulated = 0;
                (!wrapped).then_some(())?;
                wrapped = true;
                self.start
            } else {
                next
            };
        }
        if wrapped {
            self.head = self.start
        };
        let header = HeaderMut::new(self.head);
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
                let header = HeaderMut::new(self.head);
                let distance = unsafe { self.end.offset_from(self.head) } as u32;
                header.store((accumulated - capacity).min(distance));
                header.unset();
            }
        }

        Some(header)
    }
}

mod buffer;
mod header;

#[cfg(test)]
mod tests;
