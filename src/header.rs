use std::sync::atomic::{AtomicU32, Ordering::*};

pub(crate) struct Header<P>(pub(crate) P);

pub(crate) type HeaderMut = Header<*mut AtomicU32>;
pub(crate) type HeaderRo = Header<*const AtomicU32>;

impl HeaderMut {
    pub(crate) fn new(ptr: *mut u32) -> Self {
        Self(ptr as *mut AtomicU32)
    }

    pub(crate) fn set(&self) {
        unsafe { &*self.0 }.fetch_or(1, Release);
    }

    pub(crate) fn unset(&self) {
        unsafe { &*self.0 }.fetch_and(u32::MAX << 1, Release);
    }

    pub(crate) fn store(&self, size: u32) {
        unsafe { &*self.0 }.store(size << 1, Release);
    }

    pub(crate) fn capacity(&self) -> u32 {
        unsafe { &*self.0 }.load(Acquire) >> 1
    }

    pub(crate) fn available(&self) -> bool {
        (unsafe { &*self.0 }.load(Acquire) & 1) == 0
    }

    pub(crate) fn buffer(&self) -> *mut u8 {
        unsafe { self.0.add(1) as *mut u8 }
    }
}

impl HeaderRo {
    pub(crate) fn unset(&self) {
        unsafe { &*self.0 }.fetch_and(u32::MAX << 1, Release);
    }
}

impl Clone for HeaderRo {
    fn clone(&self) -> Self {
        Self(self.0)
    }
}

impl From<HeaderMut> for HeaderRo {
    fn from(value: HeaderMut) -> Self {
        Self(value.0)
    }
}
