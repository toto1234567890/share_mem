package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"
)

const (
	bufferSize = 1024 // Size of the ring buffer
)

// SharedAtomicRingBuffer represents a shared atomic ring buffer
type SharedAtomicRingBuffer struct {
	buffer   [bufferSize]atomic.Uint64 // Shared buffer
	writeIdx atomic.Uint64             // Write index
	readIdx  atomic.Uint64             // Read index
}

// NewSharedAtomicRingBuffer creates a new shared atomic ring buffer
func NewSharedAtomicRingBuffer(name string) (*SharedAtomicRingBuffer, error) {
	// Define the path for the shared memory file
	shmPath := filepath.Join("/tmp", name) // Use /tmp as a fallback for shared memory
	file, err := os.OpenFile(shmPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o666)
	if err != nil {
		return nil, fmt.Errorf("failed to open shared memory file: %v", err)
	}
	defer file.Close()

	// Set the size of the shared memory file
	size := unsafe.Sizeof(SharedAtomicRingBuffer{})
	if err := syscall.Ftruncate(int(file.Fd()), int64(size)); err != nil {
		os.Remove(shmPath) // Clean up the file if resizing fails
		return nil, fmt.Errorf("failed to set shared memory size: %v", err)
	}

	// Map the shared memory file into the process's address space
	mappedMemory, err := syscall.Mmap(int(file.Fd()), 0, int(size), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		os.Remove(shmPath) // Clean up the file if mapping fails
		return nil, fmt.Errorf("failed to map shared memory: %v", err)
	}

	// Convert the mapped memory to a SharedAtomicRingBuffer
	ringBuffer := (*SharedAtomicRingBuffer)(unsafe.Pointer(&mappedMemory[0]))
	return ringBuffer, nil
}

// SpinWait performs a low-latency CPU spin loop for the given duration
func SpinWait(duration time.Duration) {
	start := time.Now()
	for time.Since(start) < duration {
		runtime.Gosched() // Yield CPU to reduce contention
	}
}

// Write writes a value to the ring buffer
func (rb *SharedAtomicRingBuffer) Write(value uint64) bool {
	writeIdx := rb.writeIdx.Load()
	nextWriteIdx := (writeIdx + 1) % bufferSize
	// Check if the buffer is full
	for nextWriteIdx == rb.readIdx.Load() {
		SpinWait(5 * time.Microsecond)
	}
	// Write the value
	rb.buffer[writeIdx].Store(value)
	rb.writeIdx.Store(nextWriteIdx)
	return true
}

// Read reads a value from the ring buffer
func (rb *SharedAtomicRingBuffer) Read() (uint64, bool) {
	readIdx := rb.readIdx.Load()
	// Check if the buffer is empty
	for readIdx == rb.writeIdx.Load() {
		SpinWait(1 * time.Microsecond)
	}
	// Read the value
	value := rb.buffer[readIdx].Load()
	rb.readIdx.Store((readIdx + 1) % bufferSize)
	return value, true
}

func main() {
	// Create a shared atomic ring buffer
	name := "SharedRingBuffer"
	ringBuffer, err := NewSharedAtomicRingBuffer(name)
	if err != nil {
		log.Fatalf("Failed to create shared ring buffer: %v", err)
	}

	// Producer: Write data to the ring buffer
	go func() {
		for i := 0; i < 10000000; i++ {
			value := uint64(i + 1)
			ringBuffer.Write(value)
		}
	}()

	// Consumer: Read data from the ring buffer
	go func() {
		for {
			value, ok := ringBuffer.Read()
			if ok {
				fmt.Printf("Consumer: Read %d\n", value)
			}
		}
	}()

	// Wait for the producer to finish
	time.Sleep(2 * time.Second)
}

