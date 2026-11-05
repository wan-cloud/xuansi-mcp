package mcp

// errors.go — the single failure mapping every static tool crosses
// through (the go-sdk packs a handler error into a CallToolResult with
// IsError set and err.Error() as the text, which is exactly the tool
// error path we want):
//
//   - a structured daemon refusal (*server.RPCError) crosses as-is, so
//     the tool error text is exactly "<class>: <msg>" — the same line
//     the CLI prints, board classes (oom/compile/rt/api/budget/
//     timeout/protocol, ` at byte N` suffixes included) verbatim;
//   - transport-level trouble is a plain error; a dead daemon is
//     reworded so an agent reading the tool error can tell "the
//     daemon connection is gone" from "the daemon refused this";
//   - fs_write's client-side cap refusal mirrors the daemon's own
//     api-class message shape (errors.go's fs helpers) so the refusal
//     reads identically wherever it happened.

import (
	"context"
	"errors"
	"fmt"

	"xuansi/internal/apiclient"
	"xuansi/internal/server"
)

// invoke runs one internal-API call and maps its failure halves onto
// the MCP tool-error path (see the file comment).
func invoke(ctx context.Context, c *apiclient.Client, req server.Request) (map[string]any, error) {
	res, rerr, err := c.Call(ctx, req)
	if err != nil {
		if errors.Is(err, apiclient.ErrDead) {
			return nil, fmt.Errorf("daemon connection lost — restart `xuansi mcp` (or check `xuansi serve`): %w", err)
		}
		return nil, err
	}
	if rerr != nil {
		return nil, rerr // Error() renders exactly "<class>: <msg>"
	}
	return res, nil
}

// fsWriteMax mirrors the fw single-file cap (XS_FILE_MAX = 16384B) and
// the daemon's own fsFileMax refusal. The MCP face refuses ABOVE it
// before contacting the daemon — the daemon would refuse the same
// payload one hop later, so the agent learns it one hop earlier.
const fsWriteMax = 16384

// errFsTooLarge is the client-side mirror of the daemon's fs_write cap
// refusal (internal/server): same api class, same message shape —
// path, byte count, cap — with "client-side" marking where it fired.
func errFsTooLarge(path string, n int) error {
	return &server.RPCError{
		Class: "api",
		Msg:   fmt.Sprintf("fs_write %s: %d bytes exceeds the %dB single-file cap (refused client-side)", path, n, fsWriteMax),
	}
}

// errFsNotBase64 refuses a data_b64 that does not decode — the MCP
// face owns the base64 seam (the wire's data field is bytes), so the
// garbage never reaches the daemon.
func errFsNotBase64(path string, cause error) error {
	return &server.RPCError{
		Class: "api",
		Msg:   fmt.Sprintf("fs_write %s: data_b64 is not valid base64 (%v)", path, cause),
	}
}
