package domainpolicy

import "fmt"

// InvalidError is the typed error returned when a constructor rejects its
// input. Unknown or inconsistent input always fails closed, and this error
// type lets callers distinguish constructor rejections from transport or
// persistence failures.
type InvalidError struct {
	// Kind names the value being constructed (e.g. "lifecycle",
	// "security plan").
	Kind string
	// Reason describes why the value was rejected.
	Reason string
}

// Error returns the validation rejection message.
func (e *InvalidError) Error() string {
	return "domainpolicy: invalid " + e.Kind + ": " + e.Reason
}

// Is reports whether target is an InvalidError (any instance).
func (e *InvalidError) Is(target error) bool {
	_, ok := target.(*InvalidError)
	return ok
}

// ErrInvalid is the sentinel for any constructor rejection in this package.
// Callers can test with errors.Is(err, domainpolicy.ErrInvalid).
var ErrInvalid = &InvalidError{Kind: "value", Reason: "rejected"}

// newInvalid builds a typed rejection error. Kind and reason are plain
// diagnostic strings; constructor rejections never carry secrets.
func newInvalid(kind, reasonFormat string, args ...any) error {
	return &InvalidError{
		Kind:   kind,
		Reason: fmt.Sprintf(reasonFormat, args...),
	}
}
