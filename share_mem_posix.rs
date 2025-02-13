use std::ptr;
use std::slice;
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use std::thread;
use std::time::Duration;
use libc::{shm_open, mmap, munmap, close, shm_unlink};
use libc::{O_CREAT, O_RDWR, PROT_READ, PROT_WRITE, MAP_SHARED};
use std::ffi::CString;

const SHM_NAME: &str = "/ultra_low_latency_shm";
const BUFFER_SIZE: usize = 1024 * 1024; // 1 MB shared memory
const SLOT_SIZE: usize = 128;           // Fixed-size message slot
const NUM_PRODUCERS: usize = 1;         // Number of producer processes
const NUM_CONSUMERS: usize = 1;         // Number of consumer processes

/// Shared memory ring buffer
struct SharedRingBuffer {
    buffer: *mut u8,
    write_idx: AtomicUsize,
    read_idx: AtomicUsize,
}

// Explicitly implement Send and Sync for thread safety
unsafe impl Send for SharedRingBuffer {}
unsafe impl Sync for SharedRingBuffer {}

impl SharedRingBuffer {
    /// Create a new shared ring buffer
    fn new(name: &str) -> Result<Self, String> {
        let name = CString::new(name).unwrap();
        let fd = unsafe { shm_open(name.as_ptr(), O_CREAT | O_RDWR, 0o666) };
        if fd == -1 {
            return Err("Failed to create shared memory".to_string());
        }

        let addr = unsafe { mmap(ptr::null_mut(), BUFFER_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0) };
        if addr == libc::MAP_FAILED {
            return Err("Failed to map shared memory".to_string());
        }

        unsafe { close(fd) };
        Ok(Self {
            buffer: addr as *mut u8,
            write_idx: AtomicUsize::new(0),
            read_idx: AtomicUsize::new(0),
        })
    }

    /// Write a message to the ring buffer
    fn write_message(&self, message: &[u8]) -> Result<(), String> {
        if message.len() > SLOT_SIZE {
            return Err("Message too large for slot".to_string());
        }

        let write_idx = self.write_idx.load(Ordering::Acquire);
        let start = write_idx % BUFFER_SIZE;
        let end = start + SLOT_SIZE;

        if end > BUFFER_SIZE {
            return Err("Buffer overflow".to_string());
        }

        // Safe because we ensure the buffer is valid and properly synchronized
        unsafe {
            let buffer = slice::from_raw_parts_mut(self.buffer, BUFFER_SIZE);
            buffer[start..start + message.len()].copy_from_slice(message);
        }

        self.write_idx.store(write_idx + SLOT_SIZE, Ordering::Release);
        Ok(())
    }

    /// Read a message from the ring buffer
    fn read_message(&self) -> Result<Vec<u8>, String> {
        let read_idx = self.read_idx.load(Ordering::Acquire);
        let start = read_idx % BUFFER_SIZE;
        let end = start + SLOT_SIZE;

        if end > BUFFER_SIZE {
            return Err("Buffer underflow".to_string());
        }

        let message = unsafe {
            let buffer = slice::from_raw_parts(self.buffer, BUFFER_SIZE);
            buffer[start..end].to_vec()
        };        
        
        self.read_idx.store(read_idx + SLOT_SIZE, Ordering::Release);
        Ok(message)
    }
}

impl Drop for SharedRingBuffer {
    fn drop(&mut self) {
        unsafe { 
            _ = munmap(self.buffer as *mut libc::c_void, BUFFER_SIZE)
        } 
        let name = CString::new(SHM_NAME).unwrap();
        unsafe { 
            _ = shm_unlink(name.as_ptr()) 
        } 
    }
}

/// Producer thread
fn producer(id: usize, ring_buffer: Arc<SharedRingBuffer>) {
    let mut message_count = 0;
    loop {
        let message = format!("Producer {}: Message {}", id, message_count).into_bytes();
        if ring_buffer.write_message(&message).is_err() {
            eprintln!("Producer {}: Failed to write message", id);
            break;
        }
        message_count += 1;
        //thread::sleep(Duration::from_millis(10)); // Simulate work
    }
}

/// Consumer thread
fn consumer(id: usize, ring_buffer: Arc<SharedRingBuffer>) {
    loop {
        if let Ok(message) = ring_buffer.read_message() {
            println!("Consumer {}: Received: {:?}", id, String::from_utf8_lossy(&message));
        } else {
            eprintln!("Consumer {}: Failed to read message", id);
            break;
        }
        //thread::sleep(Duration::from_millis(10)); // Simulate work
    }
}

fn main() {
    // Create shared ring buffer inside Arc
    let ring_buffer = Arc::new(SharedRingBuffer::new(SHM_NAME).expect("Failed to create shared ring buffer"));

    // Spawn producer threads
    let mut producer_handles = vec![];
    for i in 0..NUM_PRODUCERS {
        let ring_buffer = Arc::clone(&ring_buffer);
        producer_handles.push(thread::spawn(move || producer(i, ring_buffer)));
    }

    // Spawn consumer threads
    let mut consumer_handles = vec![];
    for i in 0..NUM_CONSUMERS {
        let ring_buffer = Arc::clone(&ring_buffer);
        consumer_handles.push(thread::spawn(move || consumer(i, ring_buffer)));
    }

    // Wait for threads to complete
    thread::sleep(Duration::from_secs(10));
}
