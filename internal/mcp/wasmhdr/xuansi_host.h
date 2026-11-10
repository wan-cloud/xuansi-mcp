/*
 * PROVENANCE (daemon-side twin): copied VERBATIM from the
 * xuansi-drivers repo, src/xuansi_host.h, at commit
 * 1b688983c251a1a6d755113589505ecaa2dd6523 (the commit that last
 * touched it; repo HEAD at copy time cceab00). The wasm build (B11
 * task 4) expands this embedded copy into the compiler's include dir
 * so daemon-built modules compile against the SAME frozen host table
 * the drivers repo ships; T9's CI cross-leg (drivers modules rebuilt
 * through the daemon's frozen flags, hash-compared) locks the pair in
 * sync — P3-T12 re-synced the twin (the B12-T3 abi=2 i2c/cam block
 * had landed in the drivers repo after the daemon's last push; the
 * local parity leg surfaced the staleness). Edit policy: NEVER
 * hand-edit below this block — a sync is a fresh verbatim copy plus
 * an updated commit line here.
 */

/*
 * xuansi_host.h — module-side declarations of the `xuansi` native host
 * namespace (module_abi = 1, frozen surface; see README "Module ABI").
 *
 * A module imports from this table via import_module("xuansi"); the
 * board supplies the symbols at instantiation time. Any import outside
 * this table fails to link — the loader refuses the module.
 *
 * abi=1 mechanical conventions (frozen):
 *   - every host function returns void — results are written through
 *     out-parameters into the module's linear memory (the host
 *     bounds-checks the address and writes it directly; the module
 *     reads the value back as ordinary wasm memory).
 *   - buffer parameters are linear-memory offsets (u32); the host
 *     bounds-checks and translates them.
 *   - errors surface as wasm exceptions (message prefix "xuansi.<fn>").
 *
 * Native-test seam: with XUANSI_NATIVE_TEST defined the wasm import
 * attributes expand away so src/<name>/test_<name>.c can compile the
 * module with the host toolchain against a stub host. The wasm32 build
 * is unchanged.
 *
 * abi=2 (B12-T3 camera migration): the i2c x4 + cam x5 block below
 * mirrors the module-ABI native namespace revision (espressif-
 * independent; see the README "Module ABI" section) under the same
 * mechanical conventions as above. The host load gate accepts abi in
 * {1,2}. A module declares abi=2 by defining XUANSI_ABI=2 before this
 * include (the default stays 1). camera_frame_get/camera_info further
 * down are the retired abi=1 names, kept only so historical modules
 * still compile.
 */
#ifndef XUANSI_ABI
#define XUANSI_ABI 1
#endif

#if defined(__wasm__)
#define XUANSI_IMPORT(mod, fn) __attribute__((import_module(mod), import_name(fn)))
#elif defined(XUANSI_NATIVE_TEST)
#define XUANSI_IMPORT(mod, fn) /* native unit test: the stub host provides the symbols */
#else
#error "xuansi_host.h is for wasm32 targets only (define XUANSI_NATIVE_TEST for native unit tests)"
#endif

/* ---- full host table (module_abi = 1; the gpio row = set/get symbols) ---- */

/* Boot milliseconds written to *out (u32 — wraps at 49.7 days). */
XUANSI_IMPORT("xuansi", "millis")
extern void xuansi_millis(uint32_t *out);
#define millis xuansi_millis

/* Millisecond delay (host busy-waits in <=50ms slices feeding the
 * watchdog; cumulative cap of 15000ms per call — over the cap raises). */
XUANSI_IMPORT("xuansi", "delay_ms")
extern void xuansi_delay_ms(uint32_t ms);
#define delay_ms xuansi_delay_ms

/* GPIO write (first touch auto-claims the pin as output; forbidden
 * pins are refused). */
XUANSI_IMPORT("xuansi", "gpio_set")
extern void xuansi_gpio_set(uint32_t pin, uint32_t level);
#define gpio_set xuansi_gpio_set

/* GPIO read: level written to *out (first touch auto-claims as input). */
XUANSI_IMPORT("xuansi", "gpio_get")
extern void xuansi_gpio_get(uint32_t pin, uint32_t *out);
#define gpio_get xuansi_gpio_get

/* SPI master bus self-configuration (slot=0|1; mode 0..3; mosi/miso
 * must be LEGAL pins 0..47 — the v1 engine's pins_valid wants that on
 * all three bus pins, so the frozen doc's "mosi/miso=-1 = absent"
 * convention is NOT actually accepted (rc=-3 "pin banned/invalid/
 * duplicate"; the on-board B10-T6 conflict vector hit it): pass a
 * legal UNWIRED pin instead (st7789 uses 14); -1 semantics deferred
 * to abi=2. no_cs!=0 -> CS-less, no_cs=0 is REFUSED on abi=1 — the CS
 * bus is an abi=2 addition). Status 0 is written to *out (non-zero
 * would have raised before returning). */
XUANSI_IMPORT("xuansi", "spi_config")
extern void xuansi_spi_config(uint32_t *out, uint32_t slot, uint32_t sclk_pin,
                              uint32_t sclk_hz, int32_t mosi, int32_t miso,
                              uint32_t mode, uint32_t no_cs);
#define spi_config xuansi_spi_config

/* SPI full duplex: w/r are linear-memory offsets; the transferred byte
 * count is written to *out (rcap=0 = tx-only; wlen=0 = rx-only; both
 * non-zero requires them equal). */
XUANSI_IMPORT("xuansi", "spi_xfer")
extern void xuansi_spi_xfer(uint32_t *out, uint32_t slot, uint32_t wbuf_off,
                            uint32_t wlen, uint32_t rbuf_off, uint32_t rcap);
#define spi_xfer xuansi_spi_xfer

/* ---- abi=2 additions: i2c x4 + cam x5 (B12-T3 camera migration;
 * the module-ABI native namespace revision — host load gate accepts
 * abi in {1,2}; camera_frame_get/camera_info below are the RETIRED
 * abi=1 names — new modules use the cam family) ---- */

/* I2C master self-configuration: slot=0|1, sda/scl claimed with
 * prim=i2c (owner="wasm", loader session), hz 10k..1M. Status 0 is
 * written to *out (a failure raises before returning — the i2cslave
 * fixture interlocks 10/11 still apply). */
XUANSI_IMPORT("xuansi", "i2c_config")
extern void xuansi_i2c_config(uint32_t *out, uint32_t slot, uint32_t sda,
                              uint32_t scl, uint32_t hz);
#define i2c_config xuansi_i2c_config

/* I2C pure write: 7-bit addr, a single wlen <= 4096B; the written byte
 * count is written to *out. */
XUANSI_IMPORT("xuansi", "i2c_write")
extern void xuansi_i2c_write(uint32_t *out, uint32_t slot, uint32_t addr,
                             uint32_t wbuf_off, uint32_t wlen);
#define i2c_write xuansi_i2c_write

/* I2C pure read: the read byte count is written to *out. */
XUANSI_IMPORT("xuansi", "i2c_read")
extern void xuansi_i2c_read(uint32_t *out, uint32_t slot, uint32_t addr,
                            uint32_t rbuf_off, uint32_t rcap);
#define i2c_read xuansi_i2c_read

/* I2C combined transaction (repeated start): write(wbuf,wlen) ->
 * restart -> read(rbuf,rcap) — the atomic register-addressed read
 * form (same shape as SCCB_Read; the two-step fallback is module
 * code). The read byte count is written to *out. */
XUANSI_IMPORT("xuansi", "i2c_writeread")
extern void xuansi_i2c_writeread(uint32_t *out, uint32_t slot, uint32_t addr,
                                 uint32_t wbuf_off, uint32_t wlen,
                                 uint32_t rbuf_off, uint32_t rcap);
#define i2c_writeread xuansi_i2c_writeread

/* XCLK clock out on pin 15 (LCD_CAM direct drive; hz must divide
 * 160MHz with a divider <= 255 — 20MHz is canonical). The GC2145 SCCB
 * side needs a 1-2s warmup after the clock starts (the module handles
 * it with delay_ms). Status 0 is written to *out. */
XUANSI_IMPORT("xuansi", "cam_xclk")
extern void xuansi_cam_xclk(uint32_t *out, uint32_t hz);
#define cam_xclk xuansi_cam_xclk

/* Parallel capture stream start: claims the 11 DVP pins + LCD_CAM +
 * GDMA + the PSRAM frame buffer (geometry decided by the module
 * profile; if XCLK is not held it auto-starts the clock at the
 * remembered hz). This wave's only pixfmt is 0=RGB565 (anything else
 * raises). Below the conn floor = full rollback + raise. Status 0 is
 * written to *out. */
XUANSI_IMPORT("xuansi", "cam_start")
extern void xuansi_cam_start(uint32_t *out, uint32_t w, uint32_t h,
                             uint32_t pixfmt);
#define cam_start xuansi_cam_start

/* Full off: stop stream -> release the 11 DVP + XCLK pins
 * (idempotent). Status 0 is written to *out. */
XUANSI_IMPORT("xuansi", "cam_stop")
extern void xuansi_cam_stop(uint32_t *out);
#define cam_stop xuansi_cam_stop

/* Single frame copy: hold fb -> copy into buf (<=cap) -> return fb;
 * the frame's byte count / width / height are written to the len_out,
 * w_out and h_out outs (geometry rides the call). Frame over cap
 * raises; serialized with cam_start/stop via the shared camera lock
 * (an op in flight = busy, a 3s sliced wait then a retryable raise). */
XUANSI_IMPORT("xuansi", "cam_frame_get")
extern void xuansi_cam_frame_get(uint32_t *len_out, uint32_t buf_off,
                                 uint32_t cap, uint32_t *w_out,
                                 uint32_t *h_out);
#define cam_frame_get xuansi_cam_frame_get

/* Read-only probe (does not touch the sensor): {w,h,pixfmt,on} written
 * to the four outs (stream not started -> w=h=0, on=0; pixfmt 0 =
 * RGB565). */
XUANSI_IMPORT("xuansi", "cam_frame_info")
extern void xuansi_cam_frame_info(uint32_t *w_out, uint32_t *h_out,
                                  uint32_t *pixfmt_out, uint32_t *on_out);
#define cam_frame_info xuansi_cam_frame_info

/* One camera frame (raw RGB565) copied to buf_off (<=cap); the frame's
 * byte count is written to *out. The camera must already be on. */
XUANSI_IMPORT("xuansi", "camera_frame_get")
extern void xuansi_camera_frame_get(uint32_t *out, uint32_t buf_off,
                                    uint32_t cap);
#define camera_frame_get xuansi_camera_frame_get

/* Camera read-only probe: {u32 w, u32 h, u32 fs_code} (fs_code
 * 0=QVGA/1=VGA) written to out (>=12B); w=h=0 while the camera is off. */
XUANSI_IMPORT("xuansi", "camera_info")
extern void xuansi_camera_info(uint32_t *out);
#define camera_info xuansi_camera_info

/* Module-facing stream (host stamps the module name into the frame
 * preamble); the accepted byte count is written to *out. */
XUANSI_IMPORT("xuansi", "stream_send")
extern void xuansi_stream_send(uint32_t *out, uint32_t buf_off, uint32_t len);
#define stream_send xuansi_stream_send
