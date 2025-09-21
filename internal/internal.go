package internal

import (
	"fmt"
	"io"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2"
)

const ProtocolName = "ipfs"

var cidUndefSlice = []cid.Cid{cid.Undef}

func GetCarRoots(reader io.Reader, inspect bool) ([]cid.Cid, error) {
	readerAt, ok := reader.(io.ReaderAt)
	if !ok {
		return cidUndefSlice, fmt.Errorf("reader does not implement io.ReaderAt")
	}
	carReader, err := car.NewReader(readerAt)
	if err != nil {
		return cidUndefSlice, err
	}
	
	if inspect {
		_, err = carReader.Inspect(true)
		if err != nil {
			return cidUndefSlice, err
		}

	}

	roots, err := carReader.Roots()
	if err != nil {
		return cidUndefSlice, err
	}
	if len(roots) == 0 {
		return cidUndefSlice, fmt.Errorf("no roots found in CAR file")
	}
	return roots, nil
}
