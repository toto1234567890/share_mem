use std::ptr;
use std::slice;
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use std::thread;
use std::time::{Duration, Instant};
use libc::{shm_open, ftruncate, mmap, munmap, close, shm_unlink};
use libc::{O_CREAT, O_RDWR, PROT_READ, PROT_WRITE, MAP_SHARED};
use std::ffi::CString;

const SHM_NAME: &str = "/low_latency_shm";
const BUFFER_SIZE: usize = 1024 * 1024; // 1 MB shared memory
const SLOT_SIZE: usize = 128;           // Fixed-size message slot
const NUM_PRODUCERS: usize = 1;
const NUM_CONSUMERS: usize = 1;

struct SharedRingBuffer {
    buffer: *mut u8,
    write_idx: AtomicUsize,
    read_idx: AtomicUsize,
}

unsafe impl Send for SharedRingBuffer {}
unsafe impl Sync for SharedRingBuffer {}

impl SharedRingBuffer {
    fn new(name: &str) -> Result<Self, String> {
        let name = CString::new(name).unwrap();
        let fd = unsafe { shm_open(name.as_ptr(), O_CREAT | O_RDWR, 0o666) };
        if fd == -1 {
            return Err("Failed to create shared memory".to_string());
        }

        unsafe { ftruncate(fd, BUFFER_SIZE as i64) };

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

    #[inline(always)]
    fn is_full(&self) -> bool {
        let read_idx = self.read_idx.load(Ordering::Relaxed);
        let write_idx = self.write_idx.load(Ordering::Relaxed);
        (write_idx.wrapping_sub(read_idx) / SLOT_SIZE) >= (BUFFER_SIZE / SLOT_SIZE)
    }

    #[inline(always)]
    fn is_empty(&self) -> bool {
        self.read_idx.load(Ordering::Relaxed) == self.write_idx.load(Ordering::Acquire)
    }

    fn write_message(&self, message: &[u8]) -> Result<(), String> {
        while self.is_full() {
            spin_wait(Duration::from_micros(5));
        }

        let write_idx = self.write_idx.load(Ordering::Relaxed);
        let start = write_idx % BUFFER_SIZE;

        unsafe {
            let buffer = slice::from_raw_parts_mut(self.buffer, BUFFER_SIZE);
            buffer[start..start + 4].copy_from_slice(&(message.len() as u32).to_le_bytes());
            buffer[start + 4..start + 4 + message.len()].copy_from_slice(message);
        }

        self.write_idx.store(write_idx + SLOT_SIZE, Ordering::Release);
        Ok(())
    }

    fn read_message(&self) -> Result<Vec<u8>, String> {
        while self.is_empty() {
            spin_wait(Duration::from_micros(1));
        }

        let read_idx = self.read_idx.load(Ordering::Relaxed);
        let start = read_idx % BUFFER_SIZE;

        let message = unsafe {
            let buffer = slice::from_raw_parts(self.buffer, BUFFER_SIZE);
            let mut len_bytes = [0u8; 4];
            len_bytes.copy_from_slice(&buffer[start..start + 4]);
            let message_len = u32::from_le_bytes(len_bytes) as usize;

            if message_len > SLOT_SIZE - 4 {
                return Err("Corrupted message length".to_string());
            }

            buffer[start + 4..start + 4 + message_len].to_vec()
        };

        self.read_idx.store(read_idx + SLOT_SIZE, Ordering::Release);
        Ok(message)
    }
}

impl Drop for SharedRingBuffer {
    fn drop(&mut self) {
        unsafe {
            munmap(self.buffer as *mut libc::c_void, BUFFER_SIZE);
            let name = CString::new(SHM_NAME).unwrap();
            shm_unlink(name.as_ptr());
        }
    }
}

fn producer(_id: usize, ring_buffer: Arc<SharedRingBuffer>) {
    let mut message_count = 0;
    loop {
        let message = format!("Received {}", message_count).into_bytes();
        if ring_buffer.write_message(&message).is_ok() {
            message_count += 1;
        }
    }
}

fn consumer(_id: usize, ring_buffer: Arc<SharedRingBuffer>) {
    loop {
        if let Ok(message) = ring_buffer.read_message() {
            println!("{}", String::from_utf8_lossy(&message));
        }
    }
}

// Low-latency CPU spin loop
#[inline(always)]
fn spin_wait(duration: Duration) {
    let start = Instant::now();
    while start.elapsed() < duration {
        std::hint::spin_loop();
    }
}

fn main() {
    let ring_buffer = Arc::new(SharedRingBuffer::new(SHM_NAME)
        .expect("Failed to create shared ring buffer"));

    let mut producer_handles = vec![];
    for i in 0..NUM_PRODUCERS {
        let ring_buffer = Arc::clone(&ring_buffer);
        producer_handles.push(thread::spawn(move || producer(i, ring_buffer)));
    }

    let mut consumer_handles = vec![];
    for i in 0..NUM_CONSUMERS {
        let ring_buffer = Arc::clone(&ring_buffer);
        consumer_handles.push(thread::spawn(move || consumer(i, ring_buffer)));
    }

    thread::sleep(Duration::from_secs(10));
}
