package replclient

import (
	"context"
	"fmt"
)

// fsFileMax is the board's per-CHUNK cap (XS_FILE_MAX = 16384B, surfaced
// as caps limits.file_max): fs.write carries a whole file in ONE op and
// is refused above it, while the v5 chunked transport (fs.write_part
// below) moves bigger files 16384B at a time. One 16KB bin inside one
// frame sits far under the 64KB frame limit.
const fsFileMax = 16384

// FsWrite stores data at path on the board's filesystem (op fs.write).
// path must be inside the firmware whitelist (/lua/..., /drv/...);
// data above the 16384B single-op cap is refused here, client-side,
// before any bytes reach the wire — callers moving bigger files use the
// chunked trio (FsWritePart/FsWriteCommit, B10-T1).
func (c *Client) FsWrite(ctx context.Context, path string, data []byte) error {
	if len(data) > fsFileMax {
		return fmt.Errorf("replclient: fs.write %s: %d bytes exceeds the %dB single-op cap (use fs.write_part)",
			path, len(data), fsFileMax)
	}
	_, err := c.Op(ctx, "fs.write", map[string]any{"path": path, "data": data})
	return err
}

// FsWritePart appends one chunk (<=16384B) at off to the board's
// `<path>.part` staging file (v5 chunked transport). off=0 (re)opens the
// .part (truncate); off must equal the .part's current length otherwise
// (api). The reply's size is the cumulative .part length.
func (c *Client) FsWritePart(ctx context.Context, path string, off uint32, data []byte) (uint32, error) {
	res, err := c.Op(ctx, "fs.write_part", map[string]any{
		"path": path, "off": off, "data": data,
	})
	if err != nil {
		return 0, err
	}
	n, _ := res["size"].(int64)
	return uint32(n), nil
}

// FsWriteCommit atomically renames `<path>.part` to path after checking
// expected_len and (when sha256hex != "") the transport hash the board
// accumulated chunk-by-chunk. Refusals keep the .part for retry.
func (c *Client) FsWriteCommit(ctx context.Context, path string, expectedLen uint32, sha256hex string) (uint32, error) {
	args := map[string]any{"path": path, "expected_len": expectedLen}
	if sha256hex != "" {
		args["sha256"] = sha256hex
	}
	res, err := c.Op(ctx, "fs.write_commit", args)
	if err != nil {
		return 0, err
	}
	n, _ := res["size"].(int64)
	return uint32(n), nil
}

// FsReadPart reads one <=16384B window of a board file back (v5): the
// reply carries the window's bytes, the file's total, and eof. This is
// the read face for files beyond fs.read's single-op cap.
func (c *Client) FsReadPart(ctx context.Context, path string, off, length uint32) (data []byte, total uint32, eof bool, err error) {
	res, err := c.Op(ctx, "fs.read_part", map[string]any{
		"path": path, "off": off, "len": length,
	})
	if err != nil {
		return nil, 0, false, err
	}
	data, _ = res["data"].([]byte)
	t, _ := res["total"].(int64)
	eof, _ = res["eof"].(bool)
	return data, uint32(t), eof, nil
}

// FsRead reads a file back byte-exact (op fs.read; a missing file
// surfaces as a board api error).
func (c *Client) FsRead(ctx context.Context, path string) ([]byte, error) {
	res, err := c.Op(ctx, "fs.read", map[string]any{"path": path})
	if err != nil {
		return nil, err
	}
	data, _ := res["data"].([]byte)
	return data, nil
}
