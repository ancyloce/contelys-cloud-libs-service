package tokens

import "context"

// flight is one request to Identity, whose outcome every caller that waits for it shares. The
// request runs detached from the caller that started it, so that a caller that gives up spoils it
// for no one.
type flight[T any] struct {
	done  chan struct{}
	value T
	err   error
}

func newFlight[T any]() *flight[T] {
	return &flight[T]{done: make(chan struct{})}
}

// land records the request's outcome and releases every caller that waits for it.
func (f *flight[T]) land(value T, err error) {
	f.value, f.err = value, err
	close(f.done)
}

// wait returns the request's outcome, or the error of ctx when ctx ends first.
func (f *flight[T]) wait(ctx context.Context) (T, error) {
	select {
	case <-f.done:
		return f.value, f.err
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	}
}
