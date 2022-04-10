package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

// These "width" constants make up the size for each entry within the index file
var (
	offWidth uint64 = 4
	posWidth uint64 = 8
	entWidth        = offWidth + posWidth
)

// index entries will contain two fields:
// record offset
// position in the store file
type index struct {
	file *os.File    // persisted file on disk
	mmap gommap.MMap // memory mapped file for IO optimizations
	size uint64      // size of the file
}

// create a new index file, which maps metadata for where records are within the record file
func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{
		file: f,
	}
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	idx.size = uint64(fi.Size())

	// Irrespective of the file size, on initialization, we grow the memorymapped file to MaxIndexBytes
	if err = os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes)); err != nil {
		return nil, err
	}
	//TODO: Look into gommap/memory mapped files
	if idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	); err != nil {
		return nil, err
	}
	return idx, nil
}

func (i *index) Close() error {
	// Flush mmap file to disk
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}
	if err := i.file.Sync(); err != nil {
		return err
	}
	//TODO: Read about file truncating
	// If file is 1gb and you only wrote 1mb, it'll shrink the file down to i.size (ie. 1mb or however large the file is)
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}
	return i.file.Close()
}

// Read takes in an offset value and returns the position of that value for that segment within the index file
func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}

	// -1 will return the last records position
	if in == -1 {
		out = uint32((i.size / entWidth) - 1)
	} else {
		out = uint32(in)
	}

	// If you wanted the 8th value in the value it would be 8 * entWidth (simple pointer arithemetic in a sense)
	pos = uint64(out) * entWidth

	// Check to see if you go out of bounds of the available bytes within the map
	if i.size < pos+entWidth {
		return 0, 0, io.EOF
	}

	// The record offset number is a 32 bit value in the map slice starting at "pos" and ending at pos + offWidth (4 bytes)
	out = enc.Uint32(i.mmap[pos : pos+offWidth])

	// The record position is just 4 bytes after the offset number (pos+offsetWidth -> pos + entWidth)
	pos = enc.Uint64(i.mmap[pos+offWidth : pos+entWidth])
	return out, pos, nil
}

// Write will append a new offset and position value to the index file
func (i *index) Write(off uint32, pos uint64) error {
	// Ensure that the mmap doesn't exceed the size of the file after we add a new value to it
	// Example: If memory mapped file is 1gb and the size will grow to 1.1GB in the index file after writing the next record, we can't continue
	if uint64(len(i.mmap)) < i.size+entWidth {
		return io.EOF
	}

	// at the end of the file (i.size) to 4 bytes past that, let's add the offset number
	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)

	// Once we add the offset number, add the entire position
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)

	// Increase the size of the file by telling the index you added an additional entWidth bytes into the file
	i.size += uint64(entWidth)
	return nil
}

func (i *index) Name() string {
	return i.file.Name()
}
