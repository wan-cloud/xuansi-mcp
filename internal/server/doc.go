// Package server exposes the daemon's internal API to agents over a unix
// socket, using length-prefixed JSON with three message types: request,
// reply, and pushed event (a bidirectional stream).
package server
