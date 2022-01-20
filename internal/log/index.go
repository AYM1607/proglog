package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

var (
	offWidth uint64 = 4
	posWidth uint64 = 8
	entWidth        = offWidth + posWidth
)

type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{
		file: f,
	}

	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	idx.size = uint64(fi.Size())

	// File needs to be expanded to its max size as re-sizing is not possible after memory map.
	err = os.Truncate(
		f.Name(),
		int64(c.Segment.MaxIndexBytes),
	)
	if err != nil {
		return nil, err
	}

	if idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	); err != nil {
		return nil, err
	}
	return idx, nil
}

// Read takes a relative offset and returns the offset and the position of tha offset
// in the store.
// Information of the last record is returned if `in` is -1
func (i *index) Read(in int64) (off uint32, pos uint64, err error) {
	var out uint32
	if i.size == 0 {
		return 0, 0, io.EOF
	}
	if in == -1 {
		out = uint32((i.size / entWidth) - 1)
	} else {
		out = uint32(in)
	}
	idxPos := uint64(out) * entWidth

	if idxPos >= i.size {
		return 0, 0, io.EOF
	}
	off = enc.Uint32(i.mmap[idxPos : idxPos+offWidth])
	pos = enc.Uint64(i.mmap[idxPos+offWidth : idxPos+entWidth])
	return off, pos, nil
}

func (i *index) Write(off uint32, pos uint64) error {
	if i.size+entWidth > uint64(len(i.mmap)) {
		return io.EOF
	}
	idxPos := i.size
	enc.PutUint32(i.mmap[idxPos:idxPos+offWidth], off)
	enc.PutUint64(i.mmap[idxPos+offWidth:idxPos+entWidth], pos)
	i.size += entWidth
	return nil
}

func (i *index) Name() string {
	return i.file.Name()
}

func (i *index) Close() error {
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}
	if err := i.file.Sync(); err != nil {
		return err
	}
	// Truncate back to real file size after memory mapped data is synced.
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}
	return i.file.Close()
}
