use std::{
    collections::VecDeque,
    sync::{Arc, Condvar, Mutex},
};

use io::Write;

use crate::*;

const BUFSIZE: usize = 1024;
const CHUNKSIZE: usize = MINALLOCBYTES;
const TESTMSG: &[u8] = b"TEST MESSAGE 32 BYTES LONG EXACT";
const LONGMSG: &[u8] = b"THIS IS A VERY LONG MESSAGE! THIS IS A VERY LONG MESSAGE! \
                       THIS IS A VERY LONG MESSAGE! THIS IS A VERY LONG MESSAGE! \
                       THIS IS A VERY LONG MESSAGE!";
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
        let buffer = ringal.fixed(CHUNKSIZE).unwrap();
        buffers.push(buffer);
    }
    let buffer = ringal.alloc(CHUNKSIZE);
    assert!(buffer.is_none());
}

#[test]
fn test_realloc() {
    let mut ringal = Ringal::new(BUFSIZE);
    let buffer1 = ringal.fixed(BUFSIZE / 2 - U32SIZE * 2).unwrap();
    let _buffer2 = ringal.fixed(BUFSIZE / 2 - U32SIZE * 2).unwrap();
    assert!(ringal.alloc(CHUNKSIZE).is_none());
    drop(buffer1);
    assert!(ringal.alloc(CHUNKSIZE).is_some());
}

#[test]
fn test_continuous_realloc() {
    let mut ringal = Ringal::new(BUFSIZE);
    let iterations = BUFSIZE / (CHUNKSIZE + U32SIZE) * 10;
    let mut buffers = VecDeque::with_capacity(2);
    buffers.push_back(ringal.fixed(MINALLOCBYTES).unwrap());
    for i in MINALLOCBYTES..MINALLOCBYTES * 2 {
        for _ in 0..iterations {
            buffers.push_back(ringal.fixed(i).unwrap());
            buffers.pop_front();
        }
    }
}

#[test]
fn test_buffer_drop() {
    let mut ringal = Ringal::new(BUFSIZE);
    let mut buffer = ringal.fixed(TESTMSG.len()).unwrap();
    buffer.copy_from_slice(TESTMSG);
    assert_eq!(buffer.as_ref(), TESTMSG);
    assert!(ringal.fixed(BUFSIZE - TESTMSG.len()).is_none());
    drop(buffer);
    let buffer = ringal.fixed(BUFSIZE - TESTMSG.len()).unwrap();
    let buffer = buffer.freeze();
    assert!(ringal.fixed(BUFSIZE - TESTMSG.len()).is_none());
    drop(buffer);
    assert!(ringal.fixed(BUFSIZE - TESTMSG.len()).is_some());
}

#[test]
fn test_multithreaded_drops() {
    let mut ringal = Ringal::new(BUFSIZE);
    let iterations = BUFSIZE / (CHUNKSIZE + U32SIZE);
    let mut handles = Vec::with_capacity(iterations);
    let signal = Arc::new((Mutex::new(0), Condvar::new()));
    for _ in 0..iterations {
        let mut buffer = ringal.fixed(CHUNKSIZE).unwrap();
        let signal = signal.clone();
        let handle = std::thread::spawn(move || {
            buffer.copy_from_slice(TESTMSG);
            assert_eq!(buffer.as_ref(), TESTMSG);
            let mut guard = signal.0.lock().unwrap();
            *guard += 1;
            drop(signal.1.wait(guard).unwrap());
        });
        handles.push(handle);
    }
    loop {
        if *signal.0.lock().unwrap() == iterations {
            break;
        }
    }
    assert!(ringal.fixed(BUFSIZE - TESTMSG.len()).is_none());
    signal.1.notify_all();
    for h in handles {
        assert!(h.join().is_ok());
    }
    assert!(ringal.fixed(BUFSIZE / TESTMSG.len()).is_some());
}

#[test]
fn test_multithreaded_buffer_clones() {
    let mut ringal = Ringal::new(BUFSIZE);
    let mut buffer = ringal.fixed(TESTMSG.len()).unwrap();
    buffer.copy_from_slice(TESTMSG);
    let buffer = buffer.freeze();
    let clones = 64;
    let mut handles = Vec::with_capacity(clones);
    let signal = Arc::new((Mutex::new(0), Condvar::new()));
    for _ in 0..clones {
        let buffer = buffer.clone();
        let signal = signal.clone();
        let handle = std::thread::spawn(move || {
            assert_eq!(buffer.as_ref(), TESTMSG);
            let mut guard = signal.0.lock().unwrap();
            *guard += 1;
            drop(signal.1.wait(guard).unwrap());
        });
        handles.push(handle);
    }
    loop {
        if *signal.0.lock().unwrap() == clones {
            break;
        }
    }
    assert!(ringal.fixed(BUFSIZE - TESTMSG.len()).is_none());
    signal.1.notify_all();
    for h in handles {
        assert!(h.join().is_ok());
    }
    assert!(ringal.fixed(BUFSIZE / TESTMSG.len()).is_some());
}
