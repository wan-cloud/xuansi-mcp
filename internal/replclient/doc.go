// Package replclient implements the wire protocol between the daemon and
// an ESP32 board: the frame codec, the Link transport abstraction (USB
// serial and TCP), the request router, and the client operations. It is
// the Go port of the repl.py protocol knowledge and the bottom layer of
// the daemon stack, feeding discover, lease and server above it.
package replclient
