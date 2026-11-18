package mcp

// tools_fw.go — the fw distribution seat's two thin shells (dist wave
// D6): xuansi_fw_versions (the fleet reconciliation view) and
// xuansi_fw_fetch (the sha-pinned image download). Both cross the
// daemon holder-less (ro faces, zero board contact) and pass the
// daemon's replies through verbatim — the OTA pair's thin-shell
// discipline.

import (
	"context"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"xuansi/internal/server"
)

type fwVersionsIn struct {
	Version   string `json:"version,omitempty" jsonschema:"the pinned fw version to reconcile against (x.y.z, optionally -rc.N); omit or \"latest\" for the stable entry"`
	SourceURL string `json:"source_url,omitempty" jsonschema:"the distribution root override (…/releases); omit for the default images repo wan-cloud/xuansi-fw-images"`
}

type fwFetchIn struct {
	Version   string `json:"version,omitempty" jsonschema:"the pinned fw version (x.y.z); omit or \"latest\" rides the stable entry"`
	SourceURL string `json:"source_url,omitempty" jsonschema:"the distribution root override (…/releases); omit for the default images repo wan-cloud/xuansi-fw-images"`
}

// registerFW adds the fw pair (the register seam lives in
// tools_static.go).
func (ts *toolset) registerFW(srv *mcpsdk.Server) {
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        "xuansi_fw_versions",
		Description: "The firmware reconciliation view: the images repo's latest manifest (fw_version/fw_build/protocol_ver/min_daemon) against every SIGHTED board's running build — up_to_date is fw_build equality (the canonical anchor: a same-version rebuild IS a new image; ota_sha is the second check). warnings carry the compat verdict (a manifest protocol_ver above this daemon's supported face, or a min_daemon newer than this daemon — flash only after heeding them). The chain: this tool names who is behind → xuansi_fw_fetch downloads the pinned image (sha256-verified) → xuansi_ota_push flashes it → this tool re-checks up_to_date. Read-only, zero board contact, rides the daemon's ETag cache.",
	}, ts.fwVersions)
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        "xuansi_fw_fetch",
		Description: "Download one firmware image from the images repo into the daemon's fw-cache: the pinned version (x.y.z) or latest, manifest-gated and sha256-verified against bin_sha256 (a wrong sha refuses with zero board contact). The reply's path is what xuansi_ota_push flashes; the cache name self-identifies (same version+build = the second call skips the download). NEVER flashes by itself — the reboot-and-verify orchestration is xuansi_ota_push's. Check xuansi_fw_versions first (the behind-board view and the protocol_ver/min_daemon warnings).",
	}, ts.fwFetch)
}

func (ts *toolset) fwVersions(ctx context.Context, _ *mcpsdk.CallToolRequest, in fwVersionsIn) (*mcpsdk.CallToolResult, any, error) {
	res, err := ts.call(ctx, server.Request{Type: "fw_versions", Version: in.Version, URL: in.SourceURL})
	if err != nil {
		return nil, nil, err
	}
	return nil, res, nil
}

func (ts *toolset) fwFetch(ctx context.Context, _ *mcpsdk.CallToolRequest, in fwFetchIn) (*mcpsdk.CallToolResult, any, error) {
	res, err := ts.call(ctx, server.Request{Type: "fw_fetch", Version: in.Version, URL: in.SourceURL})
	if err != nil {
		return nil, nil, err
	}
	return nil, res, nil
}
