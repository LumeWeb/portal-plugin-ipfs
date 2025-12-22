package protocol

import (
	"fmt"
	"io"

	"github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2"
)

// CARBlockProcessor implements BlockProcessor for CAR files
type CARBlockProcessor struct {
	reader      *car.BlockReader
	rootsCalled bool
	DoneTracker
}

// NewCARBlockProcessor creates a new CARBlockProcessor
func NewCARBlockProcessor(r io.Reader) (*CARBlockProcessor, error) {
	cr, err := car.NewBlockReader(r)
	if err != nil {
		return nil, err
	}

	return &CARBlockProcessor{
		reader:      cr,
		DoneTracker: NewDoneTracker(),
	}, nil
}

// Next implements BlockProcessor interface
func (cp *CARBlockProcessor) Next() (blocks.Block, error) {
	// On first call, check roots and return error if none
	if !cp.rootsCalled {
		cp.rootsCalled = true
		if len(cp.reader.Roots) == 0 {
			return nil, fmt.Errorf("CAR file has no root blocks - empty or invalid CAR file")
		}
	}
	return cp.reader.Next()
}

// Roots implements BlockProcessor interface
func (cp *CARBlockProcessor) Roots() []cid.Cid {
	return cp.reader.Roots
}

// Release implements BlockProcessor interface
func (cp *CARBlockProcessor) Release() {
	// CAR BlockReader doesn't need explicit cleanup
}
