package cli

import "fmt"

type NormalInterruptError struct {
	message string
}

func (e *NormalInterruptError) Error() string {
	return e.message
}

func NormalInterruptErrorf(format string, a ...any) *NormalInterruptError {
	return &NormalInterruptError{
		message: fmt.Sprintf(format, a...),
	}
}

func (e *NormalInterruptError) Is(err error) bool {
	_, ok := err.(*NormalInterruptError)
	return ok
}
