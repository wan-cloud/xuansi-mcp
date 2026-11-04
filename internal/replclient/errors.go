package replclient

import (
	"errors"
	"fmt"
)

// ErrDead is wrapped into every error returned once the connection
// became unusable (fatal frame desync §7, the nil-id protocol frame,
// link failure, Close). Callers use errors.Is(err, ErrDead) to see that
// retrying in place is pointless — the frozen protocol has no resync,
// so the only recovery is a fresh connection on a new Link (task 8).
var ErrDead = errors.New("replclient: connection dead")

// OpError is a structured error reply from the board. Class is one of
// the six bench classes (oom/compile/rt/api/budget/timeout) or the
// frame-layer class "protocol" (§3). Msg is preserved verbatim,
// including the ` at byte N` suffix that pins a rejected token's byte
// offset in the payload.
type OpError struct {
	Op    string
	Class string
	Msg   string
}

// Error formats like the reference hub (repl.py): "<op>: error <class>: <msg>".
func (e *OpError) Error() string {
	return fmt.Sprintf("%s: error %s: %s", e.Op, e.Class, e.Msg)
}
