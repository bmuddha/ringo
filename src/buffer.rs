use std::{
    io::{self, Write},
    ops::{Deref, DerefMut},
    sync::atomic::{AtomicU32, Ordering::*},
};

use crate::{
    header::{HeaderMut, HeaderRo},
    Ringal, MAXALLOCBYTES, MINALLOCBYTES, U32SIZE,
};

pub struct BufferWriter<'a> {
    pub(crate) inner: *mut u8,
    pub(crate) header: HeaderMut,
    pub(crate) initialized: usize,
    pub(crate) capacity: usize,
    pub(crate) ringo: &'a mut Ringal,
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
    pub(crate) header: HeaderRo,
    pub(crate) inner: &'static mut [u8],
}

impl BufferMut {
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

pub struct Buffer {
    header: HeaderRo,
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

unsafe impl Send for BufferMut {}
unsafe impl Send for Buffer {}
