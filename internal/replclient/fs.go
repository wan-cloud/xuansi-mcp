package replclient

import (
	"context"
	"fmt"
)

// fsFileMax is the board's per-file cap (XS_FILE_MAX = 16384B, surfaced
// as caps limits.file_max). One fs.write carries a whole file — a 16KB
// bin inside one frame sits far under the 64KB frame limit — so there
// is no segmentation at this op face: the binding cap is the file
// limit, not the transport's.
const fsFileMax = 16384

// FsWrite stores data at path on the board's filesystem (op fs.write).
// path must be inside the firmware whitelist (/lua/..., /drv/...);
// data above the 16384B single-file cap is refused here, client-side,
// before any bytes reach the wire.
func (c *Client) FsWrite(ctx context.Context, path string, data []byte) error {
	if len(data) > fsFileMax {
		return fmt.Errorf("replclient: fs.write %s: %d bytes exceeds the %dB single-file cap",
			path, len(data), fsFileMax)
	}
	_, err := c.Op(ctx, "fs.write", map[string]any{"path": path, "data": data})
	return err
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
