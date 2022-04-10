package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	enc = binary.BigEndian
)

const (
	// represents the number of bytes used to store the length of the payload array that is of size .. size as a 64 bit integer valued (8 byte val = 64 bits)
	lenWidth = 8
)

type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

func newStore(f *os.File) (*store, error) {
	// Describe the requested file (if it exists it will not return an error)
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	// We want to store a reference for the size of the file onto the store struct
	size := uint64(fi.Size())
	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

// Append will append data (represented as a byte array) into the stores immutable log file
func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	// Prevent race conditions by putting a lock on the struct
	s.mu.Lock()
	defer s.mu.Unlock()

	// Position will be represented by the number of bytes currently stored in the file
	// If you want to append to the end of the file, it'll be pos bytes as the insertion point
	pos = s.size

	// We want to write the length of our payload in binary to the buffer
	// this allows us to understand the spec (size (bytes) - value of payload (bytes))
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}

	s.buf.Flush()
	// Write the actual payload to the file as well
	w, err := s.buf.Write(p)
	s.buf.Flush()
	if err != nil {
		return 0, 0, err
	}
	// If we wrote all of p, we know w represents the number of bytes written
	// which means adding the lenWidth for the payload size will represent total bytes written
	w += lenWidth

	// Once we have the total bytes written for the new record, we append to the total size of the store
	s.size += uint64(w)

	// w 	= total bytes written
	// pos 	= the start of the last record that was inserted (stores previous file size)
	// err 	= nil in this case because there were no issues appending the record to the store
	return uint64(w), pos, nil
}

// Read will return a record at the given position pos
func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// We want to preemptively flush the buffer in case data still remains in memory and not on disk
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}
	// Let's allocate a byte array to load up the payload size
	sizeBuffer := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(sizeBuffer, int64(pos)); err != nil {
		return nil, err
	}

	// Now that we have the size of the payload (as a byte array)
	// we need to allocate a payload array that is of size .. size as a 64 bit integer value
	payload := make([]byte, enc.Uint64(sizeBuffer))

	// We read the payload in by reading from the offset position + 8 bytes (skip the size metadata)
	if _, err := s.File.ReadAt(payload, int64(pos+lenWidth)); err != nil {
		return nil, err
	}

	return payload, nil
}

// Read at will return a record of size p at offset off if it exists
func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Preemptively flush the cache in case bytes haven't been flushed from memory to disk
	if err := s.buf.Flush(); err != nil {
		return 0, err
	}

	return s.File.ReadAt(p, off)
}

// Close will close the file that holds the records on the store struct
func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.buf.Flush()
	if err != nil {
		return err
	}
	return s.File.Close()
}
