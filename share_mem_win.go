package main

import (
	"fmt"
	"log"
	"runtime"
	"sync/atomic"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

const (
	bufferSize = 1024 // Size of the ring buffer
)

// SharedAtomicRingBuffer represents a shared atomic ring buffer
type SharedAtomicRingBuffer struct {
	buffer   [bufferSize]atomic.Uint64 // Shared buffer
	writeIdx atomic.Uint64             // Write index
	readIdx  atomic.Uint64             // Read index
	addr     uintptr                   // Base address of the shared memory
}

// NewSharedAtomicRingBuffer creates a new shared atomic ring buffer
func NewSharedAtomicRingBuffer(name string) (*SharedAtomicRingBuffer, error) {
	// Create a file mapping object
	fileMapping, err := windows.CreateFileMapping(
		windows.InvalidHandle, // Use the paging file
		nil,                   // Default security attributes
		windows.PAGE_READWRITE,
		0,
		uint32(unsafe.Sizeof(SharedAtomicRingBuffer{})),
		windows.StringToUTF16Ptr(name),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create file mapping: %v", err)
	}

	// Map the file mapping into the process's address space
	const FILE_MAP_ALL_ACCESS = windows.FILE_MAP_WRITE | windows.FILE_MAP_READ
	addr, err := windows.MapViewOfFile(
		fileMapping,
		FILE_MAP_ALL_ACCESS,
		0,
		0,
		uintptr(unsafe.Sizeof(SharedAtomicRingBuffer{})),
	)
	if err != nil {
		windows.CloseHandle(fileMapping)
		return nil, fmt.Errorf("failed to map view of file: %v", err)
	}

	// Convert the address to a SharedAtomicRingBuffer
	ringBuffer := (*SharedAtomicRingBuffer)(unsafe.Pointer(addr))

	return ringBuffer, nil
}

// SpinWait performs a low-latency CPU spin loop for the given duration
func SpinWait(duration time.Duration) {
	start := time.Now()
	for time.Since(start) < duration {
		runtime.Gosched() // Yield CPU to reduce contention
	}
}

// Close releases the shared memory resources
func (rb *SharedAtomicRingBuffer) Close() {
	windows.UnmapViewOfFile(rb.addr)
}

// Write writes a value to the ring buffer
func (rb *SharedAtomicRingBuffer) Write(value uint64) bool {
	writeIdx := rb.writeIdx.Load()
	nextWriteIdx := (writeIdx + 1) % bufferSize

	// Check if the buffer is full
	if nextWriteIdx == rb.readIdx.Load() {
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
	ringBuffer, err := NewSharedAtomicRingBuffer("SharedRingBuffer")
	if err != nil {
		log.Fatalf("Failed to create shared ring buffer: %v", err)
	}
	defer ringBuffer.Close()

	// Producer: Write data to the ring buffer
	go func() {
		for i := 0; i < 1000000; i++ {
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
