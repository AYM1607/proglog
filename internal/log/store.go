package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	// enc is the endianess used to store records.
	enc = binary.BigEndian
)

const (
	// lenWidth determines how many bytes will be used to store the length of the record.
	lenWidth = 8
)

type store struct {
	// type embedding of an os file.
	*os.File

	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

// newStore returns a ready to use store ginven a file descriptor.
func newStore(f *os.File) (*store, error) {
	// Get information for the given file descriptor.
	fi, err := os.Stat(f.Name())
	if err != nil {
		// Return nil becuase it's the zero value for a pointer.
		return nil, err
	}

	// This is useful when working with pre-existing files, which could be the case when restarting.
	size := uint64(fi.Size())
	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

// Append writes the provided bytes as a record to the end of the store.
// Returns the size fo the record and the position of the record within the store.
func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	pos = s.size
	// Write the size of the record before the actual record.
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}

	w, err := s.buf.Write(p)
	// I don't think this is the way of doing it, because Write could return an
	// error even though it wrote some bytes to the file.
	if err != nil {
		return 0, 0, err
	}
	w += lenWidth
	s.size += uint64(w)
	return uint64(w), pos, nil
}

// Read retrieves the record at position pos from the store.
func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Commit any buffered data to the file.
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	// Read the size of the record at pos.
	size := make([]byte, lenWidth)
	// Could remove `File` because of type embedding but leaving could be better for clarity?
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}

	// Read the actual record data given its offset and size.
	b := make([]byte, enc.Uint64(size))
	// Could remove `File` because of type embedding but leaving could be better for clarity?
	if _, err := s.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}
	return b, nil
}

func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return 0, err
	}

	// Could remove `File` because of type embedding but leaving could be better for clarity?
	return s.File.ReadAt(p, off)
}

func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return err
	}
	return s.File.Close()
}
