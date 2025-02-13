use std::ptr;
use std::slice;
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use std::thread;
use std::time::{Duration, Instant};
// windows specific
use windows::core::PCWSTR;
use windows::Win32::Foundation::{CloseHandle, INVALID_HANDLE_VALUE, HANDLE};
use windows::Win32::System::Memory::{
    CreateFileMappingW, MapViewOfFile, UnmapViewOfFile, MEMORY_MAPPED_VIEW_ADDRESS, FILE_MAP_ALL_ACCESS, PAGE_READWRITE,
};

const SHM_NAME: &str = "Local\\low_latency_shm";
const BUFFER_SIZE: usize = 1024 * 1024; // 1 MB shared memory
const SLOT_SIZE: usize = 128;          // Fixed-size message slot
const NUM_PRODUCERS: usize = 1;        // Number of producer processes
const NUM_CONSUMERS: usize = 1;        // Number of consumer processes

/// Shared memory ring buffer
struct SharedRingBuffer {
    buffer: ptr::NonNull<u8>,
    write_idx: AtomicUsize,
    read_idx: AtomicUsize,
    file_mapping: HANDLE,
}

// Explicitly implement Send and Sync for thread safety
unsafe impl Send for SharedRingBuffer {}
unsafe impl Sync for SharedRingBuffer {}

impl SharedRingBuffer {
    /// Create a new shared ring buffer
    fn new(name: &str) -> Result<Self, windows::core::Error> {
        let wide_name: Vec<u16> = name.encode_utf16().chain(std::iter::once(0)).collect();
        
        let file_mapping = unsafe {
            CreateFileMappingW(
                INVALID_HANDLE_VALUE,
                None,
                PAGE_READWRITE,
                0,
                BUFFER_SIZE as u32,
                PCWSTR(wide_name.as_ptr()),
            )?
        };

        if file_mapping.is_invalid() {
            return Err(windows::core::Error::from_win32());
        }

        let addr = unsafe { MapViewOfFile(file_mapping, FILE_MAP_ALL_ACCESS, 0, 0, BUFFER_SIZE) };
        if addr.Value.is_null() {
            unsafe { _ = CloseHandle(file_mapping) };
            return Err(windows::core::Error::from_win32());
        }

        // Wrap pointer in MEMORY_MAPPED_VIEW_ADDRESS
        let addr = MEMORY_MAPPED_VIEW_ADDRESS { Value: addr.Value };

        // Convert raw pointer to NonNull
        let buffer = ptr::NonNull::new(addr.Value as *mut u8)
            .ok_or_else(|| windows::core::Error::from_win32())?;

        Ok(Self {
            buffer,
            write_idx: AtomicUsize::new(0),
            read_idx: AtomicUsize::new(0),
            file_mapping,
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
            let buffer = slice::from_raw_parts_mut(self.buffer.as_ptr(), BUFFER_SIZE);
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
            let buffer = slice::from_raw_parts(self.buffer.as_ptr(), BUFFER_SIZE);
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
            _ = UnmapViewOfFile(MEMORY_MAPPED_VIEW_ADDRESS { Value: self.buffer.as_ptr() as *mut _ });
            _ = CloseHandle(self.file_mapping);
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
