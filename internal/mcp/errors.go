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
//   - fs_write's former client-side 16 KiB cap refusal is GONE (B10-T1):
//     the daemon now rides the v5 chunked transport for bigger payloads,
//     so the MCP face forwards them whole (bounded only by the internal
//     wire's 1 MiB frame cap).

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

// errFsNotBase64 refuses a data_b64 that does not decode — the MCP
// face owns the base64 seam (the wire's data field is bytes), so the
// garbage never reaches the daemon.
func errFsNotBase64(path string, cause error) error {
	return &server.RPCError{
		Class: "api",
		Msg:   fmt.Sprintf("fs_write %s: data_b64 is not valid base64 (%v)", path, cause),
	}
}

// errFsPath is the client-side path-whitelist refusal for the
// destructive fs face (fs_rm, B8): the fw's own fs gate (frozen §8:
// named files under /lua/ or /drv/ only) mirrored one hop earlier, the
// same api-class convention as the cap refusal above, so a traversal
// or root-dir path is refused before any crossing.
func errFsPath(path string) error {
	return &server.RPCError{
		Class: "api",
		Msg:   fmt.Sprintf("fs_rm %s: path must be a named file directly under /lua/ or /drv/ (no \"..\", no \".\", no \"//\", no trailing \"/\", not empty), refused client-side", path),
	}
}
