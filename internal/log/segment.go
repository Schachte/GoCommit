package log

import (
	"fmt"
	"os"
	"path"

	api_log_v1 "github.com/schachte/kafkaclone/api/v1"
	"google.golang.org/protobuf/proto"
)

type segment struct {
	store                  *store
	index                  *index
	baseOffset, nextOffset uint64
	config                 Config
}

// newSegment will generate a new segment given
// dir 			- location of the segment file
// baseOffset 	- what the base offset of the segment will be
// config 		- structure containing the segment configuration
func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	s := &segment{
		baseOffset: baseOffset,
		config:     c,
	}
	var err error
	// Open or create the user-specified segment file
	// Format is <OFFSET>.store
	storeFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}

	// Assign a new instance of a store to the given segment
	if s.store, err = newStore(storeFile); err != nil {
		return nil, err
	}

	// Create an index file that contains metadata about the record positions within the store
	indexFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")),
		os.O_RDWR|os.O_CREATE,
		0644,
	)
	if err != nil {
		return nil, err
	}

	// Assign a new index struct to the segment
	if s.index, err = newIndex(indexFile, c); err != nil {
		return nil, err
	}
	if off, _, err := s.index.Read(-1); err != nil {
		s.nextOffset = baseOffset
	} else {
		s.nextOffset = baseOffset + uint64(off) + 1
	}
	return s, nil
}

// Append will append a record to the store file of a given segment
func (s *segment) Append(record *api_log_v1.Record) (offset uint64, err error) {
	cur := s.nextOffset
	record.Offset = cur
	p, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}

	_, pos, err := s.store.Append(p)
	if err != nil {
		return 0, err
	}
	if err = s.index.Write(
		//index offsets are relative to base offset
		uint32(s.nextOffset-uint64(s.baseOffset)), pos,
	); err != nil {
		return 0, err
	}
	s.nextOffset++
	return cur, nil
}

// Read will unmarshal a record given an offset
func (s *segment) Read(off uint64) (*api_log_v1.Record, error) {
	// Find the location of the record by checking the index file
	// The only reason we subtract the baseOffset is because the user can specify a base that is a non-zero unsigned integer
	_, pos, err := s.index.Read(int64(off - s.baseOffset))
	if err != nil {
		return nil, err
	}

	// Now that we have the location of the record, we need to pull it from the store
	p, err := s.store.Read(pos)
	if err != nil {
		return nil, err
	}

	// We have the record as a byte array in p, time to marshal into a struct of api.Record
	record := &api_log_v1.Record{}
	err = proto.Unmarshal(p, record)
	return record, err
}

// IsMaxed will check:
// - the store exceeds the max store bytes or
// - the index exceeds the max index bytes
func (s *segment) IsMaxed() bool {
	return s.store.size >= s.config.Segment.MaxStoreBytes ||
		s.index.size >= s.config.Segment.MaxIndexBytes
}

// Remove will close the store and index files and delete them
func (s *segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}
	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}
	if err := os.Remove(s.store.Name()); err != nil {
		return err
	}
	return nil
}

func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}
	if err := s.store.Close(); err != nil {
		return err
	}
	return nil
}

func nearestMultiple(j, k uint64) uint64 {
	if j >= 0 {
		return (j / k) * k
	}
	return ((j - k + 1) / k) * k
}
