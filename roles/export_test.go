package roles

import "time"

// SetClock lets the tests move time past an answer's validity without waiting for it.
func SetClock(c *Client, now func() time.Time) { c.now = now }
