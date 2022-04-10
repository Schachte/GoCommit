package log

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	// dummy payload
	write = []byte("hello world")
	// entire byte length required for this record (length of value (8 bytes) + value length (11 bytes)) -> 19 bytes total
	width = uint64(len(write) + lenWidth)
)

func TestStoreAppendRead(t *testing.T) {
	f, err := ioutil.TempFile("", "store_append_read_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	// We will initialize a new store with an ephemeral record file
	s, err := newStore(f)

	testAppend(t, s)
	testRead(t, s)
	testReadAt(t, s)

	// s, err = newStore(f)
	// require.NoError(t, err)
	// testRead(t, s)
}

// Writes N records to the stores file
func testAppend(t *testing.T, s *store) {
	//TODO: Look into this helper function
	t.Helper()
	for i := uint64(1); i < 4; i++ {
		// Continuously append the same "hello world" payload

		// n 	= total bytes written
		// pos 	= the start of the last record that was inserted (stores previous file size)
		// err 	= nil in this case because there were no issues appending the record to the store
		n, pos, err := s.Append(write)
		require.NoError(t, err)

		// For each record that we write, we want to assert that
		// the position + number_bytes_written is equal to the width of the payload * the index of the record
		require.Equal(t, pos+n, width*i)
	}
}

// Reads N records from the stores file
func testRead(t *testing.T, s *store) {
	t.Helper()
	var pos uint64

	// We're going to read in the 4 payloads that we wrote in the previous test
	for i := uint64(1); i < 4; i++ {
		read, err := s.Read(pos)
		require.NoError(t, err)
		require.Equal(t, write, read)
		pos += width
	}
}

// Reads in a payload from a specific offset value
func testReadAt(t *testing.T, s *store) {
	t.Helper()
	for i, off := uint64(1), int64(0); i < 4; i++ {
		// We know that for any record we read, we first need to understand how large that record is itself
		b := make([]byte, lenWidth)
		n, err := s.ReadAt(b, off) // if offset is 0, then we will read lenWidth bytes from the file starting from the 0th byte
		require.NoError(t, err)

		// The number of bytes we read needs to equal the number of allocated bytes for the payload size in b
		require.Equal(t, lenWidth, n)

		// The offset can increase by the width of the size so we can jump to the payload itself
		off += int64(n)

		// b is a byte array (binary value representing the size of the payload)
		// converting it into a 64 bit unsigned integer will allow us to treat it like a normal number
		size := enc.Uint64(b)
		b = make([]byte, size)

		// Read the payload into the byte array
		n, err = s.ReadAt(b, off)

		require.NoError(t, err)
		// the dummy payload equals what we read
		require.Equal(t, write, b)

		// the size of the payload is equal to the number of bytes read
		require.Equal(t, int(size), n)

		// Increment the offset by the size of the payload to get to the next record
		off += int64(n)
	}
}

func TestStoreClose(t *testing.T) {
	f, err := ioutil.TempFile("", "store_close_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := newStore(f)
	require.NoError(t, err)

	// Appending data won't apply to the file until it's either read or closed
	_, _, err = s.Append(write)
	require.NoError(t, err)

	f, beforeSize, err := openFile(f.Name())
	require.NoError(t, err)

	err = s.Close()
	require.NoError(t, err)

	_, afterSize, err := openFile(f.Name())
	require.NoError(t, err)
	require.True(t, afterSize > beforeSize)
}

// openFile will open a file with a given name with r/w permissions and create if it does not exist
func openFile(name string) (file *os.File, size int64, err error) {
	f, err := os.OpenFile(
		name,
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)

	if err != nil {
		return nil, 0, err
	}

	fi, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}
	return f, fi.Size(), nil
}
