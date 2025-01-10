package db

import (
	"database/sql/driver"
	"fmt"
	"time"
)

type ViewTime struct {
	time.Time
}

// Scan implements sql.Scanner interface with enhanced database compatibility
func (ct *ViewTime) Scan(value interface{}) error {
	if value == nil {
		ct.Time = time.Time{}
		return nil
	}

	// Handle the different ways databases might return time values
	switch v := value.(type) {
	case time.Time:
		// MySQL often returns native time.Time objects
		ct.Time = v
		return nil
	case []byte:
		// Both MySQL and SQLite might return bytes
		return ct.parseTimeString(string(v))
	case string:
		// SQLite typically returns strings
		return ct.parseTimeString(v)
	default:
		// Handle any other unexpected types by converting to string
		return ct.parseTimeString(fmt.Sprintf("%v", v))
	}
}

// parseTimeString handles various time string formats
func (ct *ViewTime) parseTimeString(timeStr string) error {
	// Order these from most to least specific to ensure proper parsing
	layouts := []string{
		// SQLite formats (including the one from your error)
		"2006-01-02 15:04:05.999999999-07:00", // Full precision with timezone
		"2006-01-02 15:04:05.999999-07:00",    // Microsecond with timezone

		// MySQL formats
		"2006-01-02 15:04:05.999999", // MySQL microsecond precision
		"2006-01-02 15:04:05.999",    // MySQL millisecond precision
		"2006-01-02 15:04:05",        // MySQL basic format

		// Common formats that both might use
		time.RFC3339Nano,            // Full precision ISO8601
		time.RFC3339,                // Basic ISO8601
		"2006-01-02T15:04:05Z07:00", // ISO8601 with timezone
		"2006-01-02",                // Date only
	}

	var lastErr error
	for _, layout := range layouts {
		if t, err := time.Parse(layout, timeStr); err == nil {
			ct.Time = t
			return nil
		} else {
			lastErr = err
		}
	}

	return fmt.Errorf("could not parse time string '%s': %v", timeStr, lastErr)
}

// Value implements driver.Valuer interface
func (ct ViewTime) Value() (driver.Value, error) {
	if ct.Time.IsZero() {
		return nil, nil
	}
	// Most databases expect RFC3339 format for storing
	return ct.Time.Format("2006-01-02 15:04:05.999999"), nil
}

func (ct ViewTime) GetTime() time.Time {
	return ct.Time
}
