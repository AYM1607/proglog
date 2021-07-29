package server

import (
	"errors"
	"sync"
)

// Log represents a commit log.
type Log struct {
	mu      sync.Mutex
	records []Record
}

// NewLog returns a new ready to use Log.
func NewLog() *Log {
	return &Log{}
}

// Append adds a new record to a log.
func (c *Log) Append(record Record) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	record.Offset = uint64(len(c.records))
	c.records = append(c.records, record)
	return record.Offset, nil
}

// Read returns an individual record from the log given its offset.
// Returns an error if the offset does not exist.
func (c *Log) Read(offset uint64) (Record, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if offset >= uint64(len(c.records)) {
		return Record{}, ErrOffsetNotFound
	}

	return c.records[offset], nil
}

// Record represents a single record in a commit log.
type Record struct {
	Value  []byte `json:"value"`
	Offset uint64 `json:"offset"`
}

var ErrOffsetNotFound = errors.New("offset not found")
