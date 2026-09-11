package coolify

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Error represents a structured Coolify API error. It mirrors the useful parts
// of jefripunza/coolify-go error handling without depending on that package's
// broad handwritten client.
type Error struct {
	StatusCode int
	Message    string
	Warning    string
	Errors     map[string][]string
	Conflicts  []Conflict
	RequestID  string
	// RetryAfter carries the Retry-After hint for an HTTP 429 (rate-limited)
	// response, so the reconciler can honour the provider's requested delay.
	// It is zero when the header was absent and is never serialized.
	RetryAfter time.Duration
}

// Conflict describes a domain conflict returned with an HTTP 409.
type Conflict struct {
	Domain       string
	ResourceName string
	ResourceUUID string
	ResourceType string
	Message      string
}

func (e *Error) Error() string {
	var b strings.Builder
	if e.StatusCode != 0 {
		fmt.Fprintf(&b, "coolify: status %d", e.StatusCode)
	} else {
		b.WriteString("coolify:")
	}
	if e.Message != "" {
		fmt.Fprintf(&b, ": %s", e.Message)
	}
	if len(e.Errors) > 0 {
		keys := make([]string, 0, len(e.Errors))
		for k := range e.Errors {
			keys = append(keys, k)
		}
		fmt.Fprintf(&b, ": %s", strings.Join(keys, ","))
	}
	if len(e.Conflicts) > 0 {
		fmt.Fprintf(&b, ": %d domain conflict(s)", len(e.Conflicts))
	}
	return b.String()
}

func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	if !ok {
		return false
	}
	if t.StatusCode != 0 && e.StatusCode != t.StatusCode {
		return false
	}
	return true
}

// StatusCodeError returns a typed Error from an HTTP response. The body is
// parsed for the common Coolify error shapes. No raw body is retained.
func StatusCodeError(resp *http.Response, body []byte) error {
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	e := &Error{StatusCode: resp.StatusCode}
	_ = json.Unmarshal(body, e)
	if h := resp.Header.Get("X-Request-Id"); h != "" {
		e.RequestID = h
	}
	if secs, ok := retryAfter(resp); ok {
		e.RetryAfter = time.Duration(secs) * time.Second
	}
	return e
}

// UnmarshalJSON implements json.Unmarshaler for Error so that both the flat
// error object and Laravel-style validation objects are captured without
// reading raw bodies into logs.
func (e *Error) UnmarshalJSON(b []byte) error {
	type alias struct {
		Message   string              `json:"message"`
		Warning   string              `json:"warning"`
		Errors    map[string][]string `json:"errors"`
		Conflicts []struct {
			Domain       *string `json:"domain"`
			ResourceName *string `json:"resource_name"`
			ResourceUuid *string `json:"resource_uuid"`
			ResourceType *string `json:"resource_type"`
			Message      *string `json:"message"`
		} `json:"conflicts"`
	}
	var a alias
	if err := json.Unmarshal(b, &a); err != nil {
		return err
	}
	e.Message = a.Message
	e.Warning = a.Warning
	e.Errors = a.Errors
	for _, c := range a.Conflicts {
		e.Conflicts = append(e.Conflicts, Conflict{
			Domain:       deref(c.Domain),
			ResourceName: deref(c.ResourceName),
			ResourceUUID: deref(c.ResourceUuid),
			ResourceType: deref(c.ResourceType),
			Message:      deref(c.Message),
		})
	}
	return nil
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// readBody drains a response body and returns the bytes. Generated clients
// expose the raw body as []byte, so this is used where a body must be
// inspected before wrapping.
func readBody(resp *http.Response) ([]byte, error) {
	if resp == nil || resp.Body == nil {
		return nil, nil
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

// retryAfter parses the Retry-After header (seconds) into a duration value.
func retryAfter(resp *http.Response) (int, bool) {
	if resp == nil {
		return 0, false
	}
	v := resp.Header.Get("Retry-After")
	if v == "" {
		return 0, false
	}
	if secs, err := strconv.Atoi(v); err == nil {
		return secs, true
	}
	return 0, false
}

// IsNotFound reports whether err is a Coolify 404 error.
func IsNotFound(err error) bool {
	var e *Error
	return errors.As(err, &e) && e.StatusCode == http.StatusNotFound
}

// IsConflict reports whether err is a Coolify 409 error.
func IsConflict(err error) bool {
	var e *Error
	return errors.As(err, &e) && e.StatusCode == http.StatusConflict
}

// IsRateLimited reports whether err is a Coolify 429 error.
func IsRateLimited(err error) bool {
	var e *Error
	return errors.As(err, &e) && e.StatusCode == http.StatusTooManyRequests
}

// IsUnauthorized reports whether err is a Coolify 401 or 403 error. These are
// operator configuration problems and are not retryable.
func IsUnauthorized(err error) bool {
	var e *Error
	if !errors.As(err, &e) {
		return false
	}
	return e.StatusCode == http.StatusUnauthorized || e.StatusCode == http.StatusForbidden
}

// IsUnprocessable reports whether err is a Coolify 422 error.
func IsUnprocessable(err error) bool {
	var e *Error
	return errors.As(err, &e) && e.StatusCode == http.StatusUnprocessableEntity
}

// IsRetryableServerError reports whether err is a 5xx error.
func IsRetryableServerError(err error) bool {
	var e *Error
	return errors.As(err, &e) && e.StatusCode >= 500
}

// RetryAfterHint returns the provider-supplied Retry-After delay from a rate
// limited (429) error, or the zero duration when absent.
func RetryAfterHint(err error) time.Duration {
	var e *Error
	if !errors.As(err, &e) {
		return 0
	}
	return e.RetryAfter
}
