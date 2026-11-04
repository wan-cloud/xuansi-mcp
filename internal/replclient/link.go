package replclient

// Link is the byte pipe between the client and one board: a USB CDC
// serial port or a TCP connection — both endpoints carry the same frame
// stream (§1/§13 of the frozen protocol). Task 8 provides the concrete
// transports; tests drive the client over an in-memory pipe.
type Link interface {
	Read(p []byte) (int, error)
	Write(p []byte) (int, error)
	Close() error
	Kind() string // "usb" | "tcp"
}
