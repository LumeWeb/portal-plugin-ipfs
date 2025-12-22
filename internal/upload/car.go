package upload

import (
	"bytes"
	"io"

	carv2 "github.com/ipld/go-car/v2"
)

// detectCAR detects if the data contains a CAR file
func detectCAR(reader io.Reader) (Format, bool) {
	// Read a small buffer for CAR detection
	buf := make([]byte, 512) // CAR files have a specific header structure
	n, err := io.ReadFull(reader, buf)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return FormatUnknown, false
	}

	if n < 12 { // Minimum CAR header size
		return FormatUnknown, false
	}

	// Create a reader from the buffer for CAR validation
	bufReader := bytes.NewReader(buf[:n])

	// Try to create a CAR reader to validate the format
	_, err = carv2.NewReader(bufReader)
	if err == nil {
		return FormatCAR, true
	}

	return FormatUnknown, false
}
