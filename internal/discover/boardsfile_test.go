package discover

// boardsfile_test.go — the alias write face's lower half: the
// boards.conf line editor (boardsfile.go) and the registry's
// SetAlias/ClearAlias over it. The invariant under test is D6's
// charter — outside the target line the file does not move a byte —
// asserted four ways: against hand-written expected bytes (the curated
// corpus), structurally (exactly one line differs, and in it only the
// name), through the production parser (edit → LoadBoardsFile → the
// same aliases/pins/models plus exactly the one intended change), and
// over a seeded generator spanning every line shape the grammar has.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode"
)

// ---------------------------------------------------------------------------
// test seams
// ---------------------------------------------------------------------------

// writeConf lands content as a boards.conf under dir and answers its
// path. The file holds exactly content — a trailing newline is present
// only when content has one.
func writeConf(t *testing.T, dir, content string) string {
	t.Helper()
	path := filepath.Join(dir, "boards.conf")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write boards.conf: %v", err)
	}
	return path
}

func readConf(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return b
}

// parseConf is the production parser's verdict on a boards.conf —
// Task 1's LoadBoardsFile, the reader every editor output must
// round-trip through.
func parseConf(t *testing.T, path string) (aliases map[string]string, pins map[string]Pin, models map[string]string) {
	t.Helper()
	aliases, pins, models, err := LoadBoardsFile(path)
	if err != nil {
		t.Fatalf("LoadBoardsFile %s: %v", path, err)
	}
	return aliases, pins, models
}

// fileStamp is a file's identity for a no-write proof: its bytes plus
// the FileInfo os.SameFile compares (device + inode on unix, volume +
// file index on windows). The editor's writer is temp + rename, so a
// write that should not have happened lands on a NEW file and
// os.SameFile goes false — even where a coarse filesystem timestamp
// left the mtime untouched.
type fileStamp struct {
	data []byte
	info os.FileInfo
}

func stampFile(t *testing.T, path string) (fileStamp, bool) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return fileStamp{}, false
		}
		t.Fatalf("read %s: %v", path, err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return fileStamp{data: data, info: info}, true
}

// assertUnwritten proves a call wrote nothing: a file that existed
// keeps its bytes AND its identity (no rewrite, rename-replace
// included — so the no-op must not "write the same bytes"); a file
// that was missing is still missing.
func assertUnwritten(t *testing.T, path string, before fileStamp, existed bool) {
	t.Helper()
	now, exists := stampFile(t, path)
	if !existed {
		if exists {
			t.Fatalf("%s was created:\n%q", path, now.data)
		}
		return
	}
	if !exists {
		t.Fatalf("%s vanished", path)
	}
	if !bytes.Equal(now.data, before.data) {
		t.Fatalf("%s changed:\n before %q\n after  %q", path, before.data, now.data)
	}
	if !os.SameFile(before.info, now.info) {
		t.Fatalf("%s was rewritten onto a new file — the call must write nothing", path)
	}
	if !now.info.ModTime().Equal(before.info.ModTime()) {
		t.Fatalf("%s mtime moved: %v → %v", path, before.info.ModTime(), now.info.ModTime())
	}
}

// assertNoLeftovers proves the writer left no temp file beside the
// boards.conf (a refused or failed edit must not litter the config
// dir).
func assertNoLeftovers(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.Name() != "boards.conf" {
			t.Errorf("stray file beside boards.conf: %q", e.Name())
		}
	}
}

// naiveLines splits file bytes on "\n" the dumbest way there is — a
// "\r" stays with its line, an empty last element means the file ended
// with a newline. It shares nothing with the editor's own line model.
func naiveLines(b []byte) []string {
	return strings.Split(string(b), "\n")
}

// assertRenamed is the byte oracle for one rename: before and after
// must hold the same number of lines, differ in exactly ONE, and on
// that line only the name may change — the leading whitespace, the
// whitespace before the "=", everything from the "=" on (endpoint,
// tokens, an inline comment, trailing whitespace) and the terminator
// (a "\r", or none) ride along byte for byte.
func assertRenamed(t *testing.T, before, after []byte, oldName, alias string) {
	t.Helper()
	lb, la := naiveLines(before), naiveLines(after)
	if len(la) != len(lb) {
		t.Fatalf("line count moved: %d → %d\n before %q\n after  %q", len(lb), len(la), before, after)
	}
	var diff []int
	for i := range lb {
		if lb[i] != la[i] {
			diff = append(diff, i)
		}
	}
	if len(diff) != 1 {
		t.Fatalf("rename changed %d lines (%v), want exactly the target's:\n before %q\n after  %q",
			len(diff), diff, before, after)
	}
	i := diff[0]
	eq := strings.IndexByte(lb[i], '=')
	if eq < 0 {
		t.Fatalf("target line %q has no \"=\"", lb[i])
	}
	lead := len(lb[i]) - len(strings.TrimLeftFunc(lb[i], unicode.IsSpace))
	end := lead + len(oldName)
	if lb[i][lead:end] != oldName {
		t.Fatalf("target line %q: bytes [%d:%d) = %q, want the parser's name %q", lb[i], lead, end, lb[i][lead:end], oldName)
	}
	want := lb[i][:lead] + alias + lb[i][end:]
	if la[i] != want {
		t.Fatalf("target line after = %q\n            want = %q (only the name changes)", la[i], want)
	}
}

// ---------------------------------------------------------------------------
// the line editor: byte precision
// ---------------------------------------------------------------------------

// precisionCorpus is the hand-maintained shape the editor must not
// disturb: a header comment, blank lines (and a whitespace-only one),
// every legal line shape — pure alias, endpoint pin, key=value token,
// the tolerated spacing and mac6-case quirks, an inline `# note`
// riding a token tail (an inline comment is part of the grammar since
// the owner ruling of 2026-11-24: read as a comment, never warned
// about) — and the unknown/malformed lines the parser skips with a
// warning. Deliberately without a trailing newline.
const precisionCorpus = "" +
	"# bench boards (hand-maintained)\n" +
	"\n" +
	"bench=d75884\n" +
	"cam = 1A2B3C  \n" +
	"v6=abcdef@[fe80::1]:7777\n" +
	"spaced  =  d75887   model=spaced-1  \n" +
	"note=d75888 model=goouuu-s3cam-v1.3 # left bay, 3rd floor\n" +
	"stray line without separator\n" +
	"=d75885\n" +
	"ghost=abc\n" +
	"   \t\n" +
	"# trailing comment — d75884 sits on line 3"

func TestEditAliasBytePrecision(t *testing.T) {
	logs := captureLogs(t) // the corpus's malformed lines warn by design; the count must not move
	type rename struct{ name, mac6, alias, oldLine, newLine string }
	renames := []rename{
		{"pure alias line", "d75884", "left-bench",
			"bench=d75884", "left-bench=d75884"},
		{"endpoint pin line", "abcdef", "lab-v6",
			"v6=abcdef@[fe80::1]:7777", "lab-v6=abcdef@[fe80::1]:7777"},
		{"token line with tolerated spacing and trailing whitespace", "d75887", "sp2",
			"spaced  =  d75887   model=spaced-1  ", "sp2  =  d75887   model=spaced-1  "},
		{"token line carrying an inline # note", "d75888", "bay3",
			"note=d75888 model=goouuu-s3cam-v1.3 # left bay, 3rd floor", "bay3=d75888 model=goouuu-s3cam-v1.3 # left bay, 3rd floor"},
		{"uppercase mac6 with trailing spaces", "1a2b3c", "cam2",
			"cam = 1A2B3C  ", "cam2 = 1A2B3C  "},
	}
	for _, eol := range []struct{ name, s string }{{"LF", "\n"}, {"CRLF", "\r\n"}} {
		orig := strings.ReplaceAll(precisionCorpus, "\n", eol.s)
		for _, tc := range renames {
			t.Run(eol.name+" "+tc.name, func(t *testing.T) {
				path := writeConf(t, t.TempDir(), orig)
				before := readConf(t, path)

				start := strings.Count(logs.String(), "level=WARN")
				aliases, pins, models := parseConf(t, path)
				old, known := aliases[tc.mac6]
				if !known {
					t.Fatalf("test bug: the parser does not know %s in the corpus", tc.mac6)
				}
				afterParse := strings.Count(logs.String(), "level=WARN")

				if err := EditAlias(path, tc.mac6, tc.alias, false); err != nil {
					t.Fatalf("EditAlias(%s → %q): %v", tc.mac6, tc.alias, err)
				}

				after := readConf(t, path)
				// 1. the whole file, against hand-written expected bytes
				want := strings.Replace(orig, tc.oldLine+eol.s, tc.newLine+eol.s, 1)
				if want == orig {
					t.Fatalf("test bug: %q is not in the corpus", tc.oldLine+eol.s)
				}
				if !bytes.Equal(after, []byte(want)) {
					t.Fatalf("file drifted:\n want %q\n got  %q", want, after)
				}
				// 2. structurally: one line differs, and in it only the name
				assertRenamed(t, before, after, old, tc.alias)

				// 3. round-trip: the parser reads the same world, plus
				// exactly the one intended change
				a2, p2, m2 := parseConf(t, path)
				wantAliases := map[string]string{}
				for k, v := range aliases {
					wantAliases[k] = v
				}
				wantAliases[tc.mac6] = tc.alias
				if !reflect.DeepEqual(a2, wantAliases) {
					t.Errorf("aliases after = %#v\n want %#v", a2, wantAliases)
				}
				if !reflect.DeepEqual(p2, pins) {
					t.Errorf("pins moved: %#v → %#v", pins, p2)
				}
				if !reflect.DeepEqual(m2, models) {
					t.Errorf("models moved: %#v → %#v", models, m2)
				}
				// 4. the edited file warns exactly as much as the
				// original — the inline-note quirk is preserved, not
				// fixed, and the edit invented no new malformed line
				if warnedBefore, warnedAfter := afterParse-start, strings.Count(logs.String(), "level=WARN")-afterParse; warnedAfter != warnedBefore {
					t.Errorf("parser warnings on the edited file = %d, want the original's %d", warnedAfter, warnedBefore)
				}
				assertNoLeftovers(t, filepath.Dir(path))
			})
		}
	}

	t.Run("edits compose on one file", func(t *testing.T) {
		dir := t.TempDir()
		path := writeConf(t, dir, precisionCorpus)
		steps := []struct {
			mac6, alias string
			clear       bool
			want        string
		}{
			{"d75884", "left-bench", false, strings.Replace(precisionCorpus, "bench=d75884\n", "left-bench=d75884\n", 1)},
			{"d75888", "bay3", false, strings.Replace(strings.Replace(precisionCorpus, "bench=d75884\n", "left-bench=d75884\n", 1),
				"note=d75888 model=", "bay3=d75888 model=", 1)},
			{"d75884", "", true, strings.Replace(strings.Replace(precisionCorpus, "bench=d75884\n", "", 1),
				"note=d75888 model=", "bay3=d75888 model=", 1)},
		}
		for i, st := range steps {
			if err := EditAlias(path, st.mac6, st.alias, st.clear); err != nil {
				t.Fatalf("step %d EditAlias(%s, %q, clear=%v): %v", i, st.mac6, st.alias, st.clear, err)
			}
			if got := readConf(t, path); !bytes.Equal(got, []byte(st.want)) {
				t.Fatalf("step %d:\n want %q\n got  %q", i, st.want, got)
			}
		}
		aliases, pins, models := parseConf(t, path)
		if !reflect.DeepEqual(aliases, map[string]string{"1a2b3c": "cam", "abcdef": "v6", "d75887": "spaced", "d75888": "bay3"}) {
			t.Errorf("aliases after the sequence = %#v", aliases)
		}
		if !reflect.DeepEqual(pins, map[string]Pin{"abcdef": {Host: "fe80::1", Port: 7777}}) || !reflect.DeepEqual(models, map[string]string{"d75887": "spaced-1", "d75888": "goouuu-s3cam-v1.3"}) {
			t.Errorf("pins/models moved: %#v %#v", pins, models)
		}
	})

	// A board named on several lines: the parser lets the LAST line
	// win the alias, so the last line is the one whose name is the
	// alias — renaming any other would leave the board answering to
	// its old name after a restart. The shadowed lines are not the
	// target and do not move.
	t.Run("duplicate lines: the last line naming the board is renamed", func(t *testing.T) {
		for _, tc := range []struct{ name, orig, want string }{
			{"shadowed pure line first", "old=d75884\nother=000001\nbench=d75884 model=x\n",
				"old=d75884\nother=000001\nnew=d75884 model=x\n"},
			{"shadowed endpoint line first", "cam=d75884@10.1.1.1:7\nbench=d75884\n",
				"cam=d75884@10.1.1.1:7\nnew=d75884\n"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				path := writeConf(t, t.TempDir(), tc.orig)
				if err := EditAlias(path, "d75884", "new", false); err != nil {
					t.Fatalf("EditAlias: %v", err)
				}
				if got := readConf(t, path); !bytes.Equal(got, []byte(tc.want)) {
					t.Fatalf("file = %q, want %q", got, tc.want)
				}
				aliases, pins, models := parseConf(t, path)
				if aliases["d75884"] != "new" {
					t.Errorf("parser reads the winner as %q, want new", aliases["d75884"])
				}
				if tc.name == "shadowed pure line first" && (models["d75884"] != "x" || len(pins) != 0) {
					t.Errorf("payload = pins %#v models %#v, want the earlier line's model kept and no pin", pins, models)
				}
				if tc.name == "shadowed endpoint line first" && !reflect.DeepEqual(pins, map[string]Pin{"d75884": {Host: "10.1.1.1", Port: 7}}) {
					t.Errorf("pins = %#v, want the shadowed line's pin kept", pins)
				}
			})
		}
	})

	// A UTF-8 BOM sits inside the first line, and the parser counts it
	// as part of that line's name (it is not whitespace) — so the
	// parser's name is "\ufeffbench" and the name span includes it.
	// The rename lands exactly what the parser will read back, which
	// is what keeps memory and restart in agreement; the BOM goes with
	// the old name.
	t.Run("a leading BOM is part of the parser's name", func(t *testing.T) {
		path := writeConf(t, t.TempDir(), "\xEF\xBB\xBFbench=d75884\ncam=1a2b3c\n")
		aliases, _, _ := parseConf(t, path)
		if got := aliases["d75884"]; got != "\uFEFFbench" {
			t.Fatalf("precondition: the parser reads the BOM'd name as %q", got)
		}
		if err := EditAlias(path, "d75884", "left", false); err != nil {
			t.Fatalf("EditAlias: %v", err)
		}
		if got := readConf(t, path); !bytes.Equal(got, []byte("left=d75884\ncam=1a2b3c\n")) {
			t.Fatalf("file = %q", got)
		}
		if aliases, _, _ := parseConf(t, path); aliases["d75884"] != "left" {
			t.Errorf("parser reads %q, want left", aliases["d75884"])
		}
	})

	t.Run("setting the current name writes nothing", func(t *testing.T) {
		path := writeConf(t, t.TempDir(), precisionCorpus)
		before, existed := stampFile(t, path)
		if err := EditAlias(path, "d75888", "note", false); err != nil {
			t.Fatalf("EditAlias: %v", err)
		}
		assertUnwritten(t, path, before, existed)
	})

	t.Run("mac6 and alias are validated before anything is read", func(t *testing.T) {
		path := writeConf(t, t.TempDir(), "bench=d75884\n")
		before, existed := stampFile(t, path)
		for name, call := range map[string]func() error{
			"set, mac6 not mac-shaped":   func() error { return EditAlias(path, "zzzzzz", "cam", false) },
			"set, mac6 too short":        func() error { return EditAlias(path, "d7588", "cam", false) },
			"set, alias out of charset":  func() error { return EditAlias(path, "d75884", "Cam 1", false) },
			"set, alias empty":           func() error { return EditAlias(path, "d75884", "", false) },
			"clear, mac6 not mac-shaped": func() error { return EditAlias(path, "board", "", true) },
			"no path":                    func() error { return EditAlias("", "d75884", "cam", false) },
		} {
			if err := call(); err == nil {
				t.Errorf("%s: succeeded, want a refusal", name)
			}
		}
		assertUnwritten(t, path, before, existed)
		assertNoLeftovers(t, filepath.Dir(path))
	})
}

// ---------------------------------------------------------------------------
// the line editor: append + delete
// ---------------------------------------------------------------------------

func TestEditAliasAppendAndDelete(t *testing.T) {
	captureLogs(t)
	t.Run("append", func(t *testing.T) {
		for _, tc := range []struct{ name, orig, want string }{
			{"to an empty file", "", "bench=d75884\n"},
			{"after a final newline", "cam=1a2b3c\n", "cam=1a2b3c\nbench=d75884\n"},
			{"after a comment-only file", "# nothing yet\n\n", "# nothing yet\n\nbench=d75884\n"},
			{"CRLF file keeps CRLF", "# c\r\ncam=1a2b3c\r\n", "# c\r\ncam=1a2b3c\r\nbench=d75884\r\n"},
			// a file ending WITHOUT a newline keeps ending without one:
			// the new line lands right before the dangling last line
			{"before a dangling unterminated last line", "cam=1a2b3c", "bench=d75884\ncam=1a2b3c"},
			{"before a dangling unterminated last line in a CRLF file", "cam=1a2b3c\r\n# note", "cam=1a2b3c\r\nbench=d75884\r\n# note"},
			{"mixed endings: the new line follows the last terminator (LF)", "a=000001\r\nb=000002\n", "a=000001\r\nb=000002\nbench=d75884\n"},
			{"mixed endings: the new line follows the last terminator (CRLF)", "a=000001\nb=000002\r\n", "a=000001\nb=000002\r\nbench=d75884\r\n"},
			{"beside a malformed line that merely mentions the mac6", "bench=d75884 stray\n", "bench=d75884 stray\nbench=d75884\n"},
			{"beside a comment that mentions the mac6", "# d75884 is the left bench\n", "# d75884 is the left bench\nbench=d75884\n"},
			{"beside another board's payload line", "cam=1a2b3c@10.1.11.204:7777 model=esp32s3-dev\n",
				"cam=1a2b3c@10.1.11.204:7777 model=esp32s3-dev\nbench=d75884\n"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				path := writeConf(t, t.TempDir(), tc.orig)
				bAliases, bPins, bModels := parseConf(t, path)
				if err := EditAlias(path, "d75884", "bench", false); err != nil {
					t.Fatalf("EditAlias: %v", err)
				}
				got := readConf(t, path)
				if !bytes.Equal(got, []byte(tc.want)) {
					t.Fatalf("file = %q, want %q", got, tc.want)
				}
				// …and nothing else moved: taking the new line's bytes back
				// out gives the original, byte for byte (a pure one-line
				// insertion — the file even keeps ending, or not ending,
				// with a newline exactly as it did)
				rest := strings.Replace(string(got), "bench=d75884\r\n", "", 1)
				if rest == string(got) {
					rest = strings.Replace(string(got), "bench=d75884\n", "", 1)
				}
				if rest != tc.orig {
					t.Errorf("the file is not the original plus one line:\n orig %q\n got  %q", tc.orig, got)
				}
				// …and the parser agrees: the same world plus the one alias
				aAliases, aPins, aModels := parseConf(t, path)
				bAliases["d75884"] = "bench"
				if !reflect.DeepEqual(aAliases, bAliases) || !reflect.DeepEqual(aPins, bPins) || !reflect.DeepEqual(aModels, bModels) {
					t.Errorf("parser: aliases %#v pins %#v models %#v\n want aliases %#v pins %#v models %#v",
						aAliases, aPins, aModels, bAliases, bPins, bModels)
				}
				assertNoLeftovers(t, filepath.Dir(path))
			})
		}

		// The trailing newline is part of the file, like every other
		// byte: a file that does not end with one must still not end
		// with one after the daemon appended a line. The new line goes
		// after the last COMPLETE line — the latest spot where a whole
		// line fits without moving a byte — so the dangling last line
		// stays last, unterminated, untouched.
		t.Run("a file without a trailing newline keeps it", func(t *testing.T) {
			for _, tc := range []struct{ name, orig, want string }{
				{"a single unterminated line", "cam=1a2b3c", "bench=d75884\ncam=1a2b3c"},
				{"a dangling trailing comment", "# header\ncam=1a2b3c\n# spare", "# header\ncam=1a2b3c\nbench=d75884\n# spare"},
				{"a dangling whitespace-only line", "a=000001\n   ", "a=000001\nbench=d75884\n   "},
				{"a dangling malformed alias line", "a=000001\nstray=d75884 junk", "a=000001\nbench=d75884\nstray=d75884 junk"},
				{"CRLF throughout, dangling last line", "a=000001\r\nb=000002", "a=000001\r\nbench=d75884\r\nb=000002"},
				{"CRLF body, an LF-terminated line, then a dangling one", "a=000001\r\nb=000002\n# tail", "a=000001\r\nb=000002\nbench=d75884\n# tail"},
			} {
				t.Run(tc.name, func(t *testing.T) {
					path := writeConf(t, t.TempDir(), tc.orig)
					if err := EditAlias(path, "d75884", "bench", false); err != nil {
						t.Fatalf("EditAlias: %v", err)
					}
					got := readConf(t, path)
					if !bytes.Equal(got, []byte(tc.want)) {
						t.Fatalf("file = %q, want %q", got, tc.want)
					}
					// …which is to say: the dangling last line is still the
					// last line, byte for byte, and still unterminated
					i := strings.LastIndexByte(tc.orig, '\n')
					if dangling := tc.orig[i+1:]; dangling == "" || !bytes.HasSuffix(got, []byte(dangling)) {
						t.Errorf("the dangling last line %q did not stay last and unterminated: %q", dangling, got)
					}
					// the round trip: the parser sees the same world plus the alias
					aliases, _, _ := parseConf(t, path)
					if aliases["d75884"] != "bench" {
						t.Errorf("parser reads %#v", aliases)
					}
					if a, had := aliases["1a2b3c"]; had && a != "cam" {
						t.Errorf("the other board's alias moved: %#v", aliases)
					}
					assertNoLeftovers(t, filepath.Dir(path))
				})
			}

			// …and an unterminated TARGET line is edited where it stands:
			// renamed in place, it stays the unterminated last line.
			t.Run("an unterminated target line stays unterminated", func(t *testing.T) {
				path := writeConf(t, t.TempDir(), "a=000001\nbench=d75884")
				if err := EditAlias(path, "d75884", "cam", false); err != nil {
					t.Fatalf("EditAlias: %v", err)
				}
				if got := readConf(t, path); !bytes.Equal(got, []byte("a=000001\ncam=d75884")) {
					t.Fatalf("file = %q", got)
				}
				if err := EditAlias(path, "d75884", "", true); err != nil {
					t.Fatalf("EditAlias(clear): %v", err)
				}
				if got := readConf(t, path); !bytes.Equal(got, []byte("a=000001\n")) {
					t.Fatalf("file after the clear = %q", got)
				}
			})
		})

		t.Run("a missing file is created at the path", func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "boards.conf")
			if _, exists := stampFile(t, path); exists {
				t.Fatalf("precondition: %s exists", path)
			}
			if err := EditAlias(path, "d75884", "bench", false); err != nil {
				t.Fatalf("EditAlias: %v", err)
			}
			if got := readConf(t, path); !bytes.Equal(got, []byte("bench=d75884\n")) {
				t.Fatalf("file = %q", got)
			}
			if aliases, _, _ := parseConf(t, path); !reflect.DeepEqual(aliases, map[string]string{"d75884": "bench"}) {
				t.Errorf("parser reads %#v", aliases)
			}
		})

		t.Run("missing parent directories are created", func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "xuansi", "boards.conf")
			if err := EditAlias(path, "d75884", "bench", false); err != nil {
				t.Fatalf("EditAlias: %v", err)
			}
			if got := readConf(t, path); !bytes.Equal(got, []byte("bench=d75884\n")) {
				t.Fatalf("file = %q", got)
			}
		})

		t.Run("two boards land as two lines in call order", func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "boards.conf")
			for _, m := range []string{"1a2b3c", "d75884"} {
				if err := EditAlias(path, m, "b-"+m, false); err != nil {
					t.Fatalf("EditAlias(%s): %v", m, err)
				}
			}
			if got := readConf(t, path); !bytes.Equal(got, []byte("b-1a2b3c=1a2b3c\nb-d75884=d75884\n")) {
				t.Fatalf("file = %q", got)
			}
		})

		t.Run("the mac6 is normalized like the parser normalizes it", func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "boards.conf")
			if err := EditAlias(path, " D75884 ", "bench", false); err != nil {
				t.Fatalf("EditAlias: %v", err)
			}
			if got := readConf(t, path); !bytes.Equal(got, []byte("bench=d75884\n")) {
				t.Fatalf("file = %q", got)
			}
		})

		// A new file gets exactly the mode a plain 0644-under-the-umask
		// create gives — the mode an editor would give it, whatever the
		// host's umask is (asserted against a reference file created the
		// same way, so no umask is assumed) — never the temp writer's 0600.
		t.Run("a new file gets the mode a plain create gives", func(t *testing.T) {
			ref := filepath.Join(t.TempDir(), "reference.conf") // its own dir: boards.conf's must hold nothing else
			dir := t.TempDir()
			if err := os.WriteFile(ref, []byte("x"), 0o644); err != nil {
				t.Fatal(err)
			}
			refInfo, err := os.Stat(ref)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, "boards.conf")
			if err := EditAlias(path, "d75884", "bench", false); err != nil {
				t.Fatalf("EditAlias: %v", err)
			}
			info, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			if info.Mode().Perm() != refInfo.Mode().Perm() {
				t.Errorf("new file mode = %#o, want %#o (a plain 0644-under-umask create on this host)",
					info.Mode().Perm(), refInfo.Mode().Perm())
			}
			assertNoLeftovers(t, dir)
		})

		if runtime.GOOS != "windows" {
			t.Run("an existing file keeps its mode", func(t *testing.T) {
				path := writeConf(t, t.TempDir(), "bench=d75884\n")
				if err := os.Chmod(path, 0o600); err != nil {
					t.Fatal(err)
				}
				if err := EditAlias(path, "d75884", "cam", false); err != nil {
					t.Fatalf("EditAlias: %v", err)
				}
				info, err := os.Stat(path)
				if err != nil {
					t.Fatal(err)
				}
				if perm := info.Mode().Perm(); perm != 0o600 {
					t.Errorf("mode = %#o after the edit, want 0600", perm)
				}
			})
			t.Run("a symlinked boards.conf is followed, not replaced", func(t *testing.T) {
				dir := t.TempDir()
				real := writeConf(t, dir, "bench=d75884\n")
				link := filepath.Join(dir, "link.conf")
				if err := os.Symlink(real, link); err != nil {
					t.Skipf("symlinks unavailable here: %v", err)
				}
				if err := EditAlias(link, "d75884", "cam", false); err != nil {
					t.Fatalf("EditAlias through the symlink: %v", err)
				}
				if fi, err := os.Lstat(link); err != nil || fi.Mode()&os.ModeSymlink == 0 {
					t.Fatalf("the symlink was replaced by a regular file (%v, %v)", fi, err)
				}
				if got := readConf(t, real); !bytes.Equal(got, []byte("cam=d75884\n")) {
					t.Fatalf("the link target = %q", got)
				}
			})
		}
	})

	t.Run("delete", func(t *testing.T) {
		for _, tc := range []struct{ name, orig, want string }{
			{"middle line", "a=000001\nbench=d75884\nb=000002\n", "a=000001\nb=000002\n"},
			{"first line", "bench=d75884\nb=000002\n", "b=000002\n"},
			{"last line, terminated", "a=000001\nbench=d75884\n", "a=000001\n"},
			{"last line, unterminated", "a=000001\nbench=d75884", "a=000001\n"},
			{"only line, terminated", "bench=d75884\n", ""},
			{"only line, unterminated", "bench=d75884", ""},
			{"CRLF", "a=000001\r\nbench=d75884\r\nb=000002\r\n", "a=000001\r\nb=000002\r\n"},
			{"tolerated spacing and mac6 case are still pure", "a=000001\n  bench\t=\tD75884  \nb=000002", "a=000001\nb=000002"},
			{"beside other boards' payload lines", "cam=1a2b3c@10.1.11.204:7777 model=esp32s3-dev\nbench=d75884\n",
				"cam=1a2b3c@10.1.11.204:7777 model=esp32s3-dev\n"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				path := writeConf(t, t.TempDir(), tc.orig)
				bAliases, bPins, bModels := parseConf(t, path)
				if err := EditAlias(path, "d75884", "", true); err != nil {
					t.Fatalf("EditAlias(clear): %v", err)
				}
				got := readConf(t, path)
				if !bytes.Equal(got, []byte(tc.want)) {
					t.Fatalf("file = %q, want %q", got, tc.want)
				}
				aAliases, aPins, aModels := parseConf(t, path)
				delete(bAliases, "d75884")
				if !reflect.DeepEqual(aAliases, bAliases) || !reflect.DeepEqual(aPins, bPins) || !reflect.DeepEqual(aModels, bModels) {
					t.Errorf("parser: aliases %#v pins %#v models %#v\n want aliases %#v pins %#v models %#v",
						aAliases, aPins, aModels, bAliases, bPins, bModels)
				}
				assertNoLeftovers(t, filepath.Dir(path))
			})
		}

		t.Run("no line names the board: nothing is written", func(t *testing.T) {
			for _, orig := range []string{"a=000001\n# d75884\n", "bench=d75884 stray\n", "  \n", ""} {
				path := writeConf(t, t.TempDir(), orig)
				before, existed := stampFile(t, path)
				if err := EditAlias(path, "d75884", "", true); err != nil {
					t.Fatalf("EditAlias(clear) on %q: %v", orig, err)
				}
				assertUnwritten(t, path, before, existed)
			}
		})

		t.Run("a missing file stays missing", func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "boards.conf")
			if err := EditAlias(path, "d75884", "", true); err != nil {
				t.Fatalf("EditAlias(clear): %v", err)
			}
			assertUnwritten(t, path, fileStamp{}, false)
			assertNoLeftovers(t, dir)
		})
	})

	t.Run("failures never damage the file", func(t *testing.T) {
		t.Run("the path is a directory", func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "sub")
			if err := os.Mkdir(path, 0o755); err != nil {
				t.Fatal(err)
			}
			if err := EditAlias(path, "d75884", "bench", false); err == nil {
				t.Fatalf("editing a directory succeeded")
			}
		})
		if runtime.GOOS == "windows" || os.Geteuid() == 0 {
			t.Skip("directory permissions do not block writes here (windows ignores the read-only attribute on directories; root bypasses them)")
		}
		t.Run("an unwritable directory", func(t *testing.T) {
			dir := t.TempDir()
			path := writeConf(t, dir, "bench=d75884\n")
			if err := os.Chmod(dir, 0o500); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { os.Chmod(dir, 0o700) })
			if err := EditAlias(path, "d75884", "cam", false); err == nil {
				t.Fatalf("editing inside an unwritable directory succeeded")
			}
			if got := readConf(t, path); !bytes.Equal(got, []byte("bench=d75884\n")) {
				t.Fatalf("the original was damaged: %q", got)
			}
			if err := EditAlias(filepath.Join(dir, "new.conf"), "d75884", "cam", false); err == nil {
				t.Fatalf("creating a file inside an unwritable directory succeeded")
			}
		})
	})
}

// TestEditAliasWriteFailures: the writer's promises under failure —
// the original is never lost, nothing litters the config directory —
// at the two steps a filesystem can refuse after the edit itself
// succeeded.
func TestEditAliasWriteFailures(t *testing.T) {
	logs := captureLogs(t)
	origReplace, origFsync := replaceBoardsFile, fsyncBoardsDir
	t.Cleanup(func() { replaceBoardsFile, fsyncBoardsDir = origReplace, origFsync })

	t.Run("the rename fails: the original is whole and the refusal says so", func(t *testing.T) {
		replaceBoardsFile = func(string, string) error { return errors.New("sharing violation") }
		dir := t.TempDir()
		path := writeConf(t, dir, "bench=d75884\n")
		before, existed := stampFile(t, path)
		err := EditAlias(path, "d75884", "cam", false)
		if err == nil || !strings.Contains(err.Error(), "unchanged") {
			t.Fatalf("EditAlias: err = %v, want a refusal saying the file is unchanged", err)
		}
		assertUnwritten(t, path, before, existed)
		assertNoLeftovers(t, dir)
	})

	// By the directory fsync the file IS replaced and its data
	// flushed; reporting a failure here would leave the caller's
	// memory out of step with a file that did change.
	t.Run("the directory fsync fails: the write stands, with a warning", func(t *testing.T) {
		replaceBoardsFile = origReplace
		fsyncBoardsDir = func(string) error { return errors.New("fsync: invalid argument") }
		dir := t.TempDir()
		path := writeConf(t, dir, "bench=d75884\n")
		if err := EditAlias(path, "d75884", "cam", false); err != nil {
			t.Fatalf("EditAlias: %v — a directory-fsync failure after a successful rename must not fail the write", err)
		}
		if got := readConf(t, path); !bytes.Equal(got, []byte("cam=d75884\n")) {
			t.Fatalf("file = %q", got)
		}
		if !strings.Contains(logs.String(), "could not be fsynced") {
			t.Errorf("the best-effort fsync failure was swallowed silently; want it logged")
		}
		assertNoLeftovers(t, dir)
	})
}

// TestEditAliasNewFileRidesTheSameWriter: the first alias (a missing
// file) is written by the same discipline as every later edit — temp
// file, fsync, rename, directory fsync — never a direct write that a
// crash could leave half-written. A failing rename creates nothing; a
// temp file a crashed write left behind is swept once it is stale (a
// live one is never touched); a dangling symlink is followed.
func TestEditAliasNewFileRidesTheSameWriter(t *testing.T) {
	origReplace, origFsync := replaceBoardsFile, fsyncBoardsDir
	t.Cleanup(func() { replaceBoardsFile, fsyncBoardsDir = origReplace, origFsync })

	t.Run("a failing rename creates nothing and leaves nothing", func(t *testing.T) {
		replaceBoardsFile = func(string, string) error { return errors.New("sharing violation") }
		dir := t.TempDir()
		path := filepath.Join(dir, "boards.conf")
		err := EditAlias(path, "d75884", "bench", false)
		if err == nil || !strings.Contains(err.Error(), "unchanged") {
			t.Fatalf("EditAlias on a missing file: err = %v, want a refusal naming the unchanged file", err)
		}
		assertUnwritten(t, path, fileStamp{}, false)
		assertNoLeftovers(t, dir)
	})

	t.Run("the directory is fsynced for a new file too", func(t *testing.T) {
		replaceBoardsFile = origReplace
		var synced []string
		fsyncBoardsDir = func(d string) error { synced = append(synced, d); return nil }
		dir := t.TempDir()
		path := filepath.Join(dir, "boards.conf")
		if err := EditAlias(path, "d75884", "bench", false); err != nil {
			t.Fatalf("EditAlias: %v", err)
		}
		if len(synced) != 1 || synced[0] != dir {
			t.Errorf("directory fsyncs = %q, want exactly [%s]", synced, dir)
		}
		if got := readConf(t, path); !bytes.Equal(got, []byte("bench=d75884\n")) {
			t.Errorf("file = %q", got)
		}
		assertNoLeftovers(t, dir)
	})

	t.Run("a stale temp file a crashed write left behind is swept; a live one is not", func(t *testing.T) {
		dir := t.TempDir()
		stale := filepath.Join(dir, ".boards.conf-crashed.tmp")
		live := filepath.Join(dir, ".boards.conf-inflight.tmp")
		for f, content := range map[string]string{stale: "half-written", live: "being written"} {
			if err := os.WriteFile(f, []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		old := time.Now().Add(-2 * time.Hour)
		if err := os.Chtimes(stale, old, old); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(dir, "boards.conf")
		if err := EditAlias(path, "d75884", "bench", false); err != nil {
			t.Fatalf("EditAlias: %v", err)
		}
		if _, err := os.Stat(stale); !os.IsNotExist(err) {
			t.Errorf("the stale temp file is still there (err = %v)", err)
		}
		if got, err := os.ReadFile(live); err != nil || string(got) != "being written" {
			t.Errorf("the live temp file was touched: %q, %v", got, err)
		}
		if got := readConf(t, path); !bytes.Equal(got, []byte("bench=d75884\n")) {
			t.Errorf("file = %q", got)
		}
	})

	if runtime.GOOS != "windows" {
		t.Run("a dangling symlink is followed on first creation", func(t *testing.T) {
			dir := t.TempDir()
			if err := os.Mkdir(filepath.Join(dir, "real"), 0o755); err != nil {
				t.Fatal(err)
			}
			link := filepath.Join(dir, "link.conf")
			if err := os.Symlink(filepath.Join("real", "boards.conf"), link); err != nil {
				t.Skipf("symlinks unavailable here: %v", err)
			}
			if err := EditAlias(link, "d75884", "bench", false); err != nil {
				t.Fatalf("EditAlias through a dangling symlink: %v", err)
			}
			if fi, err := os.Lstat(link); err != nil || fi.Mode()&os.ModeSymlink == 0 {
				t.Fatalf("the symlink was replaced by a regular file (%v, %v)", fi, err)
			}
			if got := readConf(t, filepath.Join(dir, "real", "boards.conf")); !bytes.Equal(got, []byte("bench=d75884\n")) {
				t.Fatalf("the link target = %q", got)
			}
			assertNoLeftovers(t, filepath.Join(dir, "real"))
		})

		// The same dangling link under a SYMLINKED config directory, its
		// relative target climbing out with `..`. The kernel reads a
		// relative target from the link's REAL directory:
		//   <dir>/real/../shared/boards.conf  =  <dir>/shared/boards.conf
		// Joining the target onto the path as SPELLED reads it from
		// somewhere else — `<dir>/link/conf/../shared` cleans to
		// `<dir>/link/shared`, a directory that never existed, which the
		// writer then created and reported success over: the file landed
		// where nothing that reads the link would ever look.
		t.Run("a dangling symlink with a relative `..` target under a symlinked directory lands where the kernel resolves it", func(t *testing.T) {
			dir := t.TempDir()
			if err := os.Mkdir(filepath.Join(dir, "real"), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(filepath.Join(dir, "link"), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(filepath.Join("..", "real"), filepath.Join(dir, "link", "conf")); err != nil {
				t.Skipf("symlinks unavailable here: %v", err)
			}
			path := filepath.Join(dir, "link", "conf", "boards.conf")
			if err := os.Symlink(filepath.Join("..", "shared", "boards.conf"), path); err != nil {
				t.Skipf("symlinks unavailable here: %v", err)
			}
			if err := EditAlias(path, "d75884", "bench", false); err != nil {
				t.Fatalf("EditAlias through the dangling symlink: %v", err)
			}
			want := filepath.Join(dir, "shared", "boards.conf")
			if got, err := os.ReadFile(want); err != nil || string(got) != "bench=d75884\n" {
				t.Errorf("the link target = %q, %v; want the file where the kernel resolves the link (%s)", got, err, want)
			}
			if _, err := os.Lstat(filepath.Join(dir, "link", "shared")); !os.IsNotExist(err) {
				t.Errorf("a `shared` directory was created beside the symlinked config directory — the lexical join's landing spot (err = %v)", err)
			}
			// …and reading the link — what the next daemon start does —
			// finds it: the link stayed a link, and it resolves now
			if fi, err := os.Lstat(path); err != nil || fi.Mode()&os.ModeSymlink == 0 {
				t.Fatalf("the symlink was replaced by a regular file (%v, %v)", fi, err)
			}
			if got, err := os.ReadFile(path); err != nil || string(got) != "bench=d75884\n" {
				t.Errorf("reading %s through the link = %q, %v", path, got, err)
			}
			assertNoLeftovers(t, filepath.Join(dir, "shared"))
		})
	}
}

// ---------------------------------------------------------------------------
// the line editor: unalias deletes ONLY a pure alias line
// ---------------------------------------------------------------------------

func TestClearAliasRefusesPayloadLines(t *testing.T) {
	captureLogs(t)
	for _, tc := range []struct{ name, line, alias string }{
		{"an @endpoint pin", "cam=d75884@10.1.11.204:7777", "cam"},
		{"a model= token", "bench=d75884 model=goouuu-s3cam-v1.3", "bench"},
		{"an unknown key=value token", "bench=d75884 foo=bar", "bench"},
		{"an inline # note on a token tail", "bench=d75884 model=x # left bay", "bench"},
		{"a bare @ (a malformed endpoint)", "bench=d75884@", "bench"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := writeConf(t, dir, "# header\n"+tc.line+"\nother=000001\n")
			before, existed := stampFile(t, path)

			err := EditAlias(path, "d75884", "", true)
			if !errors.Is(err, ErrLineHasPayload) {
				t.Fatalf("EditAlias(clear) on %q: err = %v, want ErrLineHasPayload", tc.line, err)
			}
			// the refusal names the file and the line, and coaches the ONE
			// edit that clears the alias without losing the payload
			// …spelled out for THIS line: which name, to what, and how
			// from the seat
			for _, want := range []string{path, fmt.Sprintf("%q", tc.line), "rename the name to the board's mac6", fmt.Sprintf("%q → %q", tc.alias, "d75884"), "the rest of the line unchanged", "or by setting the alias to d75884"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("refusal %q lacks %q", err, want)
				}
			}
			assertSafeCoach(t, err.Error())
			assertUnwritten(t, path, before, existed)
			assertNoLeftovers(t, dir)
		})
	}

	t.Run("a board named on two lines", func(t *testing.T) {
		for _, tc := range []struct{ name, orig string }{
			{"two pure lines", "old=d75884\nother=000001\nbench=d75884\n"},
			{"a payload line then a pure line", "cam=d75884@10.1.1.1:7\nbench=d75884\n"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				dir := t.TempDir()
				path := writeConf(t, dir, tc.orig)
				before, existed := stampFile(t, path)
				err := EditAlias(path, "d75884", "", true)
				if !errors.Is(err, ErrLineHasPayload) {
					t.Fatalf("err = %v, want ErrLineHasPayload — deleting one line would leave the other naming the board", err)
				}
				if !strings.Contains(err.Error(), "2 lines") || !strings.Contains(err.Error(), "rename the name on each of them to the board's mac6 (d75884) by hand") {
					t.Errorf("refusal %q must say the board is named on 2 lines and coach renaming each name to the mac6 by hand", err)
				}
				assertSafeCoach(t, err.Error())
				assertUnwritten(t, path, before, existed)
			})
		}
	})

	// What IS deletable: `name=mac6` and nothing else — the parser's
	// tolerated spellings of exactly that included.
	t.Run("every tolerated spelling of a pure alias line is deletable", func(t *testing.T) {
		for _, pure := range []string{"bench=d75884", "bench = d75884", "  bench\t=\tD75884  ", "bench=d75884   "} {
			dir := t.TempDir()
			path := writeConf(t, dir, "# header\n"+pure+"\nother=000001\n")
			if err := EditAlias(path, "d75884", "", true); err != nil {
				t.Fatalf("clear on %q: %v, want the pure line deleted", pure, err)
			}
			if got := readConf(t, path); !bytes.Equal(got, []byte("# header\nother=000001\n")) {
				t.Errorf("clear on %q: file = %q", pure, got)
			}
		}
	})
}

// deleteNameInstructions are phrasings that would send a reader to
// remove a line's NAME — the one edit that destroys the payload D6
// protects (a nameless line parses to nothing, pin and model= included).
// No unalias refusal may carry one.
var deleteNameInstructions = []string{
	"delete the name", "delete the line", "delete it yourself", "delete the name yourself",
	"remove the name", "remove the line", "by deleting", "deleting the name", "deleting the line", "deleting one",
}

// assertSafeCoach fails when an unalias message instructs the
// destructive edit, and requires the safe one: rename the name to the
// board's mac6.
func assertSafeCoach(t *testing.T, msg string) {
	t.Helper()
	lower := strings.ToLower(msg)
	for _, bad := range deleteNameInstructions {
		if strings.Contains(lower, bad) {
			t.Errorf("the coach instructs the destructive edit (%q):\n %q", bad, msg)
		}
	}
	if !strings.Contains(msg, "rename the name") || !strings.Contains(msg, "mac6") {
		t.Errorf("the coach lacks the safe recipe (rename the name to the board's mac6):\n %q", msg)
	}
	// …and it must lead somewhere: a coach of the form `"d75884" →
	// "d75884"` sends the reader where they already are. An agent that
	// follows it calls unalias again, gets the same coach back, and
	// loops — the terminal state the fix wave closed (a line already
	// named by the mac6 now clears as the idempotent success).
	if i := strings.Index(msg, "\" → \""); i >= 0 {
		from := msg[strings.LastIndex(msg[:i], "\"")+1 : i]
		rest := msg[i+len("\" → \""):]
		if j := strings.IndexByte(rest, '"'); j >= 0 && from == rest[:j] {
			t.Errorf("the coach sends the reader where they already are (%q → %q):\n %q", from, from, msg)
		}
	}
}

// TestUnaliasCoachIsTheSafeEdit: a boards.conf line needs its name —
// the premise, probed through the production parser. Without it (the
// name deleted, or the whole line gone) the line's @endpoint and
// model= parse to NOTHING: only a warning at the next start. So an
// unalias refusal must send the reader to the one edit that clears an
// alias off a payload line and keeps the payload — renaming the name
// to the board's own mac6 — and never to the edit that loses it. The
// recipe is then followed through the registry: the alias set to the
// mac6 renames the name and nothing else, now and after a restart —
// and that is the END of the recipe: the clear that was refused is
// then the idempotent success.
func TestUnaliasCoachIsTheSafeEdit(t *testing.T) {
	captureLogs(t)
	const payload = "@10.1.11.204:7777 model=esp32s3-dev"
	wantPins := map[string]Pin{"1a2b3c": {Host: "10.1.11.204", Port: 7777}}
	wantModels := map[string]string{"1a2b3c": "esp32s3-dev"}
	for _, tc := range []struct {
		name, line string
		parses     bool
	}{
		{"intact", "cam=1a2b3c" + payload, true},
		{"the name deleted", "=1a2b3c" + payload, false},
		{"the `name=` removed", "1a2b3c" + payload, false},
		{"the line gone", "", false},
		{"the name renamed to the mac6", "1a2b3c=1a2b3c" + payload, true},
	} {
		t.Run("premise: "+tc.name, func(t *testing.T) {
			aliases, pins, models, _ := LoadBoardsFile(writeConf(t, t.TempDir(), tc.line+"\n"))
			if !tc.parses {
				if len(aliases)+len(pins)+len(models) != 0 {
					t.Fatalf("%q parses to aliases %#v pins %#v models %#v — want nothing (the premise of the coach)", tc.line, aliases, pins, models)
				}
				return
			}
			if !reflect.DeepEqual(pins, wantPins) || !reflect.DeepEqual(models, wantModels) {
				t.Fatalf("%q parses to pins %#v models %#v, want the payload intact", tc.line, pins, models)
			}
		})
	}

	const conf = "# rig\ncam=1a2b3c" + payload + "\n"
	w := newAliasWorld(t, conf, worldBoard("1a2b3c", "xuansi-1a2b3c.local"))

	// the refusal: the safe recipe, nothing destructive, nothing written
	before, existed := stampFile(t, w.conf)
	err := w.reg.ClearAlias("1a2b3c")
	if !errors.Is(err, ErrLineHasPayload) {
		t.Fatalf("ClearAlias(1a2b3c) = %v, want ErrLineHasPayload", err)
	}
	assertSafeCoach(t, err.Error())
	assertUnwritten(t, w.conf, before, existed)

	// the recipe, from the registry: the alias set to the board's own
	// mac6 renames the name — the rest of the line, byte for byte
	if err := w.reg.SetAlias("1a2b3c", "1a2b3c"); err != nil {
		t.Fatalf("SetAlias(1a2b3c, its own mac6) — the recipe the coach names: %v", err)
	}
	if got := readConf(t, w.conf); !bytes.Equal(got, []byte("# rig\n1a2b3c=1a2b3c"+payload+"\n")) {
		t.Fatalf("file = %q, want the name renamed and the payload untouched", got)
	}
	// the board now answers to its mac6 (as an unaliased board does),
	// its pin and model intact — in memory AND after a restart
	for name, r := range map[string]*Registry{"the running registry": w.reg, "a restart": w.rebuild()} {
		if got := aliasOf(r, "1a2b3c"); got != "1a2b3c" {
			t.Errorf("%s: alias = %q, want the mac6", name, got)
		}
		b, err := r.Resolve("1a2b3c")
		if err != nil || b.MAC6 != "1a2b3c" || b.Port != 7777 || b.IP != "10.1.11.204" || b.Model != "esp32s3-dev" || b.ModelSource != ModelSourceConf {
			t.Errorf("%s: Resolve(1a2b3c) = %+v, %v — want the pin and the conf model intact", name, b, err)
		}
	}
	// …and the recipe is the END of it. The line is still a payload line
	// — unalias never deletes it — but it carries no alias any more: a
	// clear on it is the idempotent success the Desc promises, never
	// the same refusal again with a coach that goes nowhere (until the
	// fix wave it kept refusing, with `"1a2b3c" → "1a2b3c"`, and an
	// agent following the coach looped). Nothing is written.
	renamed := "# rig\n1a2b3c=1a2b3c" + payload + "\n"
	before, existed = stampFile(t, w.conf)
	for i := 1; i <= 2; i++ { // idempotent: the second call answers the same
		if err := w.reg.ClearAlias("1a2b3c"); err != nil {
			t.Fatalf("ClearAlias #%d after the rename = %v, want nil (nothing left to clear)", i, err)
		}
		assertUnwritten(t, w.conf, before, existed)
	}
	if got := readConf(t, w.conf); !bytes.Equal(got, []byte(renamed)) {
		t.Fatalf("file = %q, want the recipe's line untouched:\n       %q", got, renamed)
	}
	// …and memory is what the file says, which is what a restart reads:
	// the line keeps the mac6 as its name (the parser has no "name ==
	// mac6 means no alias" rule — that is a separate ruling), so the
	// board answers to its mac6 live and after a restart, pin and model
	// intact.
	for name, r := range map[string]*Registry{"the running registry": w.reg, "a restart": w.rebuild()} {
		if got := aliasOf(r, "1a2b3c"); got != "1a2b3c" {
			t.Errorf("%s: alias = %q, want the mac6 (the file's name)", name, got)
		}
		b, err := r.Resolve("1a2b3c")
		if err != nil || b.MAC6 != "1a2b3c" || b.Port != 7777 || b.IP != "10.1.11.204" || b.Model != "esp32s3-dev" || b.ModelSource != ModelSourceConf {
			t.Errorf("%s: Resolve(1a2b3c) = %+v, %v — want the pin and the conf model intact", name, b, err)
		}
	}
}

// TestUnaliasAfterAHandRenameToTheMac6: the same end state reached by
// hand — the file's line renamed to the mac6 while the daemon still
// holds the old alias in memory. The clear succeeds, writes nothing,
// and the memory follows the file: the stale alias is gone from this
// daemon too (it no longer resolves), and what remains is exactly what
// a restart reads.
func TestUnaliasAfterAHandRenameToTheMac6(t *testing.T) {
	captureLogs(t)
	const payload = "@10.1.11.204:7777 model=esp32s3-dev"
	w := newAliasWorld(t, "cam=1a2b3c"+payload+"\n", worldBoard("1a2b3c", "xuansi-1a2b3c.local"))
	if got := aliasOf(w.reg, "1a2b3c"); got != "cam" {
		t.Fatalf("precondition: alias = %q, want cam", got)
	}
	if err := os.WriteFile(w.conf, []byte("1a2b3c=1a2b3c"+payload+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	before, existed := stampFile(t, w.conf)
	if err := w.reg.ClearAlias("1a2b3c"); err != nil {
		t.Fatalf("ClearAlias on a line the file already names by the mac6 = %v, want nil", err)
	}
	assertUnwritten(t, w.conf, before, existed)
	if got, want := aliasOf(w.reg, "1a2b3c"), aliasOf(w.rebuild(), "1a2b3c"); got != "1a2b3c" || want != "1a2b3c" {
		t.Fatalf("after the clear: memory %q, a restart %q — want the file's name (the mac6) in both", got, want)
	}
	if _, err := w.reg.Resolve("cam"); err == nil {
		t.Errorf("the stale alias cam still resolves after the clear")
	}
	if _, err := w.reg.Resolve("1a2b3c"); err != nil {
		t.Errorf("the board no longer answers to its mac6: %v", err)
	}
}

// ---------------------------------------------------------------------------
// the line editor: the END of the safe recipe — a line already named by
// the mac6 has no alias left to clear
// ---------------------------------------------------------------------------

// TestClearAliasOnAMac6NamedLineIsANoop: the end of the safe recipe,
// at the editor. The coach sends the reader to rename the name to the
// board's own mac6; once that is done the line carries no alias, and a
// clear on it is the idempotent success — not the same refusal again,
// with a coach that now says `"d75884" → "d75884"` (the terminal loop:
// an agent following the coach never gets out). Nothing is written: the
// line keeps its payload, and the parser reads it as before. The
// predicate lives in the payload branch only — a PURE line named by the
// mac6 is still deleted — and a name that merely looks like the mac6
// (another case) is not there yet: its coach still leads somewhere.
func TestClearAliasOnAMac6NamedLineIsANoop(t *testing.T) {
	captureLogs(t)
	const payload = "@10.1.11.204:7777 model=esp32s3-dev"
	for _, tc := range []struct{ name, line string }{
		{"an @endpoint", "d75884=d75884@10.1.11.204:7777"},
		{"a model= token", "d75884=d75884 model=esp32s3-dev"},
		{"an @endpoint and a model=", "d75884=d75884" + payload},
		{"an unknown key", "d75884=d75884 foo=bar"},
		{"an inline comment", "d75884=d75884 # left bay"},
		{"a token tail and an inline comment", "d75884=d75884 model=x # left bay"},
		{"leading whitespace and an uppercase mac6 value", "  d75884 = D75884  model=esp32s3-dev"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := writeConf(t, dir, "# rig\n"+tc.line+"\nother=000001\n")
			before, existed := stampFile(t, path)
			a0, p0, m0 := parseConf(t, path)
			for i := 1; i <= 2; i++ { // idempotent: the second call answers the same
				if err := EditAlias(path, "d75884", "", true); err != nil {
					t.Fatalf("clear #%d on %q = %v, want nil (nothing left to clear)", i, tc.line, err)
				}
				assertUnwritten(t, path, before, existed)
			}
			assertNoLeftovers(t, dir)
			if a, p, m := parseConf(t, path); !reflect.DeepEqual(a, a0) || !reflect.DeepEqual(p, p0) || !reflect.DeepEqual(m, m0) {
				t.Fatalf("the parser's verdict moved on an untouched file:\n before %#v %#v %#v\n after  %#v %#v %#v", a0, p0, m0, a, p, m)
			}
		})
	}

	t.Run("an uppercase name is not the mac6 yet: the coach still leads somewhere", func(t *testing.T) {
		dir := t.TempDir()
		path := writeConf(t, dir, "D75884=d75884"+payload+"\n")
		before, existed := stampFile(t, path)
		err := EditAlias(path, "d75884", "", true)
		if !errors.Is(err, ErrLineHasPayload) {
			t.Fatalf("clear on an uppercase name = %v, want ErrLineHasPayload (the name is not the mac6 yet)", err)
		}
		for _, want := range []string{`"D75884" → "d75884"`, "or by setting the alias to d75884"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("refusal %q lacks %q", err, want)
			}
		}
		assertSafeCoach(t, err.Error())
		assertUnwritten(t, path, before, existed)
	})

	t.Run("a pure line named by the mac6 still goes", func(t *testing.T) {
		dir := t.TempDir()
		path := writeConf(t, dir, "# rig\nd75884=d75884\nother=000001\n")
		if err := EditAlias(path, "d75884", "", true); err != nil {
			t.Fatalf("clear on a pure line named by the mac6 = %v, want the line deleted", err)
		}
		if got := readConf(t, path); !bytes.Equal(got, []byte("# rig\nother=000001\n")) {
			t.Errorf("file = %q", got)
		}
	})

	// The same end state by another road: the board named on several
	// lines, every name already the mac6 (the multi-line coach says
	// "rename the name on each of them" — done, it must not refuse
	// again with the same nowhere-leading coach).
	for _, tc := range []struct{ name, conf string }{
		{"a pure line and a payload line", "d75884=d75884\nd75884=d75884" + payload + "\n"},
		{"two payload lines", "d75884=d75884@10.1.11.204:7777\nd75884=d75884 model=esp32s3-dev\n"},
	} {
		t.Run("several lines, each already named by the mac6: "+tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := writeConf(t, dir, tc.conf)
			before, existed := stampFile(t, path)
			if err := EditAlias(path, "d75884", "", true); err != nil {
				t.Fatalf("clear = %v, want nil (no line carries an alias any more)", err)
			}
			assertUnwritten(t, path, before, existed)
			assertNoLeftovers(t, dir)
		})
	}
	t.Run("several lines, one still under another name: still refused", func(t *testing.T) {
		dir := t.TempDir()
		path := writeConf(t, dir, "old=d75884\nd75884=d75884"+payload+"\n")
		before, existed := stampFile(t, path)
		err := EditAlias(path, "d75884", "", true)
		if !errors.Is(err, ErrLineHasPayload) || !strings.Contains(err.Error(), "2 lines") {
			t.Fatalf("clear = %v, want the ErrLineHasPayload refusal naming 2 lines (line 1 is still an alias)", err)
		}
		assertSafeCoach(t, err.Error())
		assertUnwritten(t, path, before, existed)
	})
}

// TestInlineCommentThroughTheEditor: an inline comment on each line
// shape, round-tripped through the editor (owner ruling 2026-11-24: `#`
// comments are supported inline on every line shape).
//
//   - the line NAMES its board — nameSpan is the parser's own verdict —
//     and a set/rename moves the name field and nothing else: the
//     comment survives byte for byte, the whitespace around it included;
//   - the parser then reads the edited file as the alias plus whatever
//     the MEANINGFUL part carries (pin, model) — the comment ignored,
//     and none of its words warned about;
//   - the line is STILL not deletable: a comment is not payload, but
//     unalias does not delete a line that carries anything after the
//     mac6 slot. The refusal is the conservative safe recipe, and the
//     file is untouched.
func TestInlineCommentThroughTheEditor(t *testing.T) {
	logs := captureLogs(t)
	warns := func() int { return strings.Count(logs.String(), "level=WARN") }
	for _, tc := range []struct {
		name, line string
		pins       map[string]Pin
		models     map[string]string
		warnings   int // what the tokens BEFORE the comment earn
	}{
		{"alias-only", "cam=d75884 # left bay, 3rd floor", map[string]Pin{}, map[string]string{}, 0},
		{"alias-only, a bare #", "cam=d75884 #", map[string]Pin{}, map[string]string{}, 0},
		{"pinned", "cam=d75884@10.1.11.204:7777 # left bay", map[string]Pin{"d75884": {Host: "10.1.11.204", Port: 7777}}, map[string]string{}, 0},
		{"a model= token", "cam=d75884 model=esp32s3-dev # left bay", map[string]Pin{}, map[string]string{"d75884": "esp32s3-dev"}, 0},
		{"a commented-out model= after a live one", "cam=d75884 model=esp32s3-dev # model=commented-out-2", map[string]Pin{}, map[string]string{"d75884": "esp32s3-dev"}, 0},
		{"a pin and only a commented-out model=", "cam=d75884@10.1.11.204:7777 # model=commented-out-2", map[string]Pin{"d75884": {Host: "10.1.11.204", Port: 7777}}, map[string]string{}, 0},
		{"an unknown key and a stray word before the comment", "cam=d75884 foo=bar stray # note", map[string]Pin{}, map[string]string{}, 2},
	} {
		for _, eol := range []struct{ name, s string }{{"LF", "\n"}, {"CRLF", "\r\n"}} {
			t.Run(tc.name+" ("+eol.name+")", func(t *testing.T) {
				dir := t.TempDir()
				orig := "# rig" + eol.s + tc.line + eol.s + "other=000001" + eol.s
				path := writeConf(t, dir, orig)
				before, existed := stampFile(t, path)

				// names its board, and is not a deletable line
				if start, end, mac6, ok := nameSpan(tc.line); !ok || mac6 != "d75884" || tc.line[start:end] != "cam" {
					t.Fatalf("nameSpan(%q) = %d,%d,%q,%v — want the name field of a line that names d75884", tc.line, start, end, mac6, ok)
				}
				if isPureAlias(tc.line) {
					t.Fatalf("isPureAlias(%q) = true — a comment-bearing line must not be deletable", tc.line)
				}
				// setting the name it already has: not a byte is written
				if err := EditAlias(path, "d75884", "cam", false); err != nil {
					t.Fatalf("no-op set: %v", err)
				}
				assertUnwritten(t, path, before, existed)

				// the rename: the name field, nothing else — the comment
				// survives byte for byte
				if err := EditAlias(path, "d75884", "bay3", false); err != nil {
					t.Fatalf("rename: %v", err)
				}
				renamed := strings.Replace(tc.line, "cam=d75884", "bay3=d75884", 1)
				want := "# rig" + eol.s + renamed + eol.s + "other=000001" + eol.s
				if got := readConf(t, path); !bytes.Equal(got, []byte(want)) {
					t.Fatalf("file = %q\n     want %q (only the name moved; the comment as written)", got, want)
				}
				assertRenamed(t, []byte(orig), []byte(want), "cam", "bay3")
				assertNoLeftovers(t, dir)

				// …and the production parser reads it back: the alias, the
				// meaningful part's pin and model, the comment ignored
				w0 := warns()
				aliases, pins, models := parseConf(t, path)
				if !reflect.DeepEqual(aliases, map[string]string{"d75884": "bay3", "000001": "other"}) {
					t.Errorf("aliases = %#v", aliases)
				}
				if !reflect.DeepEqual(pins, tc.pins) || !reflect.DeepEqual(models, tc.models) {
					t.Errorf("pins/models = %#v %#v, want %#v %#v", pins, models, tc.pins, tc.models)
				}
				if got := warns() - w0; got != tc.warnings {
					t.Errorf("warnings from the edited file = %d, want %d — a word behind the '#' warned, or a real one went missing:\n%s", got, tc.warnings, logs.String())
				}

				// unalias: the line carries a comment, so it is not
				// deleted — refused with the safe recipe, file untouched
				before, existed = stampFile(t, path)
				err := EditAlias(path, "d75884", "", true)
				if !errors.Is(err, ErrLineHasPayload) {
					t.Fatalf("clear on %q = %v, want ErrLineHasPayload (a comment-bearing line is not deletable)", renamed, err)
				}
				for _, want := range []string{path, strconv.Quote(renamed), `"bay3" → "d75884"`, "the rest of the line unchanged", "or by setting the alias to d75884"} {
					if !strings.Contains(err.Error(), want) {
						t.Errorf("refusal %q lacks %q", err, want)
					}
				}
				assertSafeCoach(t, err.Error())
				assertUnwritten(t, path, before, existed)
				assertNoLeftovers(t, dir)
			})
		}
	}
}

// nameSpanVerdicts — TestNameSpanOnInlineCommentLines pins the editor's
// two per-line verdicts on the inline-comment spellings of the ruled
// grammar, and the look-alikes it does not accept: whatever names a
// board is the parser's own reading (they ride one splitter), and
// nothing that carries a comment is pure.
func TestNameSpanOnInlineCommentLines(t *testing.T) {
	for _, tc := range []struct {
		line  string
		names bool
		name  string
		pure  bool
	}{
		{"cam=d75884 # note", true, "cam", false},
		{"  cam = D75884\t# note  ", true, "cam", false},
		{"cam=d75884@10.1.11.204:7777 # note", true, "cam", false},
		{"cam=d75884 model=x # note", true, "cam", false},
		{"cam=d75884 # model=x", true, "cam", false},
		{"cam=d75884#", false, "", false},             // the '#' glued to the mac6 value is no comment: the value is `d75884#`, not a mac6
		{"cam=d75884 stray # note", false, "", false}, // the stray word: still part of the head, still malformed
		{"cam=d75884 model=a#b", true, "cam", false},  // a '#' inside a token is no comment: an illegal model value
		{"# cam=d75884 # note", false, "", false},     // a comment line
		{"cam=d75884", true, "cam", true},             // no comment at all: the pure line, deletable
	} {
		start, end, mac6, ok := nameSpan(tc.line)
		if ok != tc.names || (ok && (mac6 != "d75884" || tc.line[start:end] != tc.name)) {
			t.Errorf("nameSpan(%q) = %d,%d,%q,%v, want names=%v as %q", tc.line, start, end, mac6, ok, tc.names, tc.name)
		}
		if got := isPureAlias(tc.line); got != tc.pure {
			t.Errorf("isPureAlias(%q) = %v, want %v", tc.line, got, tc.pure)
		}
	}
}

// ---------------------------------------------------------------------------
// the registry API: validate against the world → edit → sync
// ---------------------------------------------------------------------------

func worldBoard(mac6, host string) Board {
	return Board{
		MAC6: mac6, Sources: []string{SourceUSB, SourceMDNS},
		USBPath: "/dev/serial/by-id/fake-" + mac6, Host: host, Port: 7777,
	}
}

// aliasWorld is a registry over a real boards file plus an injected
// discovery world the test can grow — a board "discovered later".
type aliasWorld struct {
	t      *testing.T
	dir    string
	conf   string
	boards []Board
	reg    *Registry
}

func newAliasWorld(t *testing.T, conf string, boards ...Board) *aliasWorld {
	t.Helper()
	w := &aliasWorld{t: t, dir: t.TempDir(), boards: boards}
	w.conf = writeConf(t, w.dir, conf)
	w.reg = w.rebuild()
	return w
}

// rebuild is what a daemon restart does: parse the file, build the
// registry over it, run one sweep.
func (w *aliasWorld) rebuild() *Registry {
	w.t.Helper()
	aliases, pins, models, err := LoadBoardsFile(w.conf)
	if err != nil {
		w.t.Fatal(err)
	}
	reg := NewRegistry(aliases, pins,
		WithModels(models),
		WithBoardsFile(w.conf),
		WithUSB(func(context.Context) ([]Board, error) { return w.boards, nil }),
		WithMDNS(func(context.Context) ([]Board, error) { return w.boards, nil }),
	)
	if err := reg.Refresh(context.Background()); err != nil {
		w.t.Fatalf("Refresh: %v", err)
	}
	return reg
}

func (w *aliasWorld) sweep() {
	w.t.Helper()
	if err := w.reg.Refresh(context.Background()); err != nil {
		w.t.Fatalf("Refresh: %v", err)
	}
}

// aliasOf reads a board's alias straight off the registry view (no
// sweep, no resolve: what list_boards would show right now).
func aliasOf(r *Registry, mac6 string) string {
	for _, b := range r.Snapshot() {
		if b.MAC6 == mac6 {
			return b.Alias
		}
	}
	return ""
}

func knownTo(r *Registry, mac6 string) bool {
	for _, b := range r.Snapshot() {
		if b.MAC6 == mac6 {
			return true
		}
	}
	return false
}

// TestSetAliasValidation: the write-time refusals, each with its
// coach, each leaving the file and the memory untouched. The world:
// d75884 (alias bench), 1a2b3c (alias cam) are discovered; abcdef
// (alias lab) is known from the file but has never been seen; the
// target d75885 is discovered with no line yet.
func TestSetAliasValidation(t *testing.T) {
	captureLogs(t)
	w := newAliasWorld(t, "# bench\nbench=d75884\ncam=1a2b3c\nlab=abcdef\n",
		worldBoard("d75884", "xuansi-d75884.local"),
		worldBoard("1a2b3c", "xuansi-1a2b3c.local"),
		worldBoard("d75885", "xuansi-d75885.local"))
	before, existed := stampFile(t, w.conf)
	if knownTo(w.reg, "abcdef") {
		t.Fatalf("precondition: abcdef must be file-known and never seen")
	}

	refuse := func(alias, coach string) {
		t.Helper()
		err := w.reg.SetAlias("d75885", alias)
		if err == nil {
			t.Fatalf("SetAlias(d75885, %q) succeeded, want a refusal", alias)
		}
		if !strings.Contains(err.Error(), coach) {
			t.Errorf("SetAlias(d75885, %q):\n refusal = %q\n lacks   %q", alias, err, coach)
		}
		assertUnwritten(t, w.conf, before, existed)
		if got := aliasOf(w.reg, "d75885"); got != "" {
			t.Errorf("a refused alias reached the registry view: %q", got)
		}
	}

	// charset: ^[a-z0-9][a-z0-9_-]{0,31}$
	const charset = "^[a-z0-9][a-z0-9_-]{0,31}$"
	for _, tc := range []struct{ alias, why string }{
		{"Bench", "an uppercase letter"},
		{"bench.1", "a '.' outside the charset"},
		{"板1", "a non-ASCII letter"},
		{"-bench", "a leading '-'"},
		{"_bench", "a leading '_'"},
		{"bench 1", "a space"},
		{"", "nothing"},
		{strings.Repeat("b", 33), "33 characters (one past the ceiling)"},
	} {
		t.Run("charset "+tc.why, func(t *testing.T) { refuse(tc.alias, charset) })
	}

	// another board's alias — the refusal names the rule that fired,
	// not merely a word the generic coach happens to contain
	t.Run("collides with another board's alias", func(t *testing.T) {
		refuse("cam", "is already board 1a2b3c's alias")
	})
	// …a board the file knows though no discovery face has ever seen it
	t.Run("collides with a never-seen board's alias", func(t *testing.T) {
		refuse("lab", "is already board abcdef's alias")
	})

	// a known mac6 — whole, or as a prefix (selectors match mac6 prefixes)
	t.Run("collides with a known mac6 (whole)", func(t *testing.T) {
		refuse("d75884", "is a prefix of known mac6 d75884")
	})
	t.Run("collides with a known mac6 (prefix)", func(t *testing.T) {
		refuse("d7", "is a prefix of known mac6 d75884")
	})
	// a never-seen board that holds ANOTHER alias: still the mac6 rule
	// (its alias "lab" is not what "abcde" collides with)
	t.Run("collides with a never-seen board's mac6 (prefix)", func(t *testing.T) {
		refuse("abcde", "is a prefix of known mac6 abcdef")
	})

	// a known hostname prefix (the prefix itself must be a legal alias
	// — '.' is not in the charset, so it stops before the ".local")
	t.Run("collides with a hostname prefix", func(t *testing.T) {
		// a prefix of EVERY hostname names the first board in mac6 order
		refuse("xuansi", `is a prefix of board 1a2b3c's hostname "xuansi-1a2b3c.local"`)
	})
	t.Run("collides with a longer hostname prefix", func(t *testing.T) {
		refuse("xuansi-d75884", `is a prefix of board d75884's hostname "xuansi-d75884.local"`)
	})

	// every uniqueness refusal carries the way out and the no-write note
	t.Run("the coach rides every uniqueness refusal", func(t *testing.T) {
		for _, alias := range []string{"cam", "d7", "xuansi"} {
			err := w.reg.SetAlias("d75885", alias)
			if err == nil {
				t.Fatalf("SetAlias(d75885, %q) succeeded, want a refusal", alias)
			}
			for _, coach := range []string{"exactly one board", "pick a name", "nothing was written", `alias "` + alias + `"`} {
				if !strings.Contains(err.Error(), coach) {
					t.Errorf("refusal for %q lacks %q:\n %q", alias, coach, err)
				}
			}
		}
	})

	// a refused validation is not a mac6 problem in disguise
	t.Run("the target must be a mac6", func(t *testing.T) {
		for _, mac6 := range []string{"zzzzzz", "d7588", "", "board"} {
			if err := w.reg.SetAlias(mac6, "bench2"); err == nil || !strings.Contains(err.Error(), "mac6") {
				t.Errorf("SetAlias(%q, bench2) = %v, want a mac6 refusal", mac6, err)
			}
			if err := w.reg.ClearAlias(mac6); err == nil || !strings.Contains(err.Error(), "mac6") {
				t.Errorf("ClearAlias(%q) = %v, want a mac6 refusal", mac6, err)
			}
		}
		assertUnwritten(t, w.conf, before, existed)
	})
}

// TestSetAliasLifecycle: the accepted writes — validate against the
// world, edit the file, sync the memory — and the invariant that
// binds them: after every successful call the registry says exactly
// what a daemon restart would read back from the file.
func TestSetAliasLifecycle(t *testing.T) {
	captureLogs(t)
	w := newAliasWorld(t, "# bench\nbench=d75884\ncam=1a2b3c@10.1.11.204:7777 model=esp32s3-dev\n",
		worldBoard("d75884", "xuansi-d75884.local"),
		worldBoard("1a2b3c", "xuansi-1a2b3c.local"),
		worldBoard("d75885", "xuansi-d75885.local"))

	// the alias a board already has: a no-op, and not a single byte
	// is written (the file keeps its identity)
	t.Run("setting the current alias is a no-op", func(t *testing.T) {
		before, existed := stampFile(t, w.conf)
		if err := w.reg.SetAlias("d75884", "bench"); err != nil {
			t.Fatalf("SetAlias(current): %v", err)
		}
		assertUnwritten(t, w.conf, before, existed)
	})

	// rename: immediate, no sweep needed
	t.Run("rename is immediate and resolves", func(t *testing.T) {
		if err := w.reg.SetAlias("d75884", "left"); err != nil {
			t.Fatalf("SetAlias: %v", err)
		}
		if got := aliasOf(w.reg, "d75884"); got != "left" {
			t.Fatalf("alias right after SetAlias = %q, want left (no sweep)", got)
		}
		if b, err := w.reg.Resolve("left"); err != nil || b.MAC6 != "d75884" {
			t.Errorf("Resolve(left) = %v, %v; want d75884", b.MAC6, err)
		}
		if _, err := w.reg.Resolve("bench"); err == nil {
			t.Errorf("Resolve(bench) still resolves after the rename")
		}
		if got := readConf(t, w.conf); !bytes.Equal(got, []byte("# bench\nleft=d75884\ncam=1a2b3c@10.1.11.204:7777 model=esp32s3-dev\n")) {
			t.Fatalf("file = %q", got)
		}
		// a sweep re-applies the alias map: the rename survives
		w.sweep()
		if got := aliasOf(w.reg, "d75884"); got != "left" {
			t.Errorf("alias after a sweep = %q, want left", got)
		}
		// …and it is exactly what a restart reads
		if got := aliasOf(w.rebuild(), "d75884"); got != "left" {
			t.Errorf("alias after a restart = %q, want left", got)
		}
	})

	// append: a board with no line gets one
	t.Run("a board with no line gets a line appended", func(t *testing.T) {
		if err := w.reg.SetAlias("d75885", "spare"); err != nil {
			t.Fatalf("SetAlias: %v", err)
		}
		want := "# bench\nleft=d75884\ncam=1a2b3c@10.1.11.204:7777 model=esp32s3-dev\nspare=d75885\n"
		if got := readConf(t, w.conf); !bytes.Equal(got, []byte(want)) {
			t.Fatalf("file = %q\n     want %q", got, want)
		}
		if got := aliasOf(w.reg, "d75885"); got != "spare" {
			t.Fatalf("alias = %q, want spare", got)
		}
	})

	// the mac6 is normalized on the way in, like Pin's
	t.Run("a padded uppercase mac6 is normalized", func(t *testing.T) {
		if err := w.reg.SetAlias(" D75884 ", "left2"); err != nil {
			t.Fatalf("SetAlias: %v", err)
		}
		if got := aliasOf(w.reg, "d75884"); got != "left2" {
			t.Fatalf("alias = %q, want left2", got)
		}
	})

	// the 32-character ceiling is inside the domain
	t.Run("a 32-character alias is accepted", func(t *testing.T) {
		long := strings.Repeat("l", 32)
		if err := w.reg.SetAlias("d75885", long); err != nil {
			t.Fatalf("SetAlias(32 chars): %v", err)
		}
		if got := aliasOf(w.reg, "d75885"); got != long {
			t.Errorf("alias = %q", got)
		}
	})

	// clear: a pure alias line goes, and only that
	t.Run("clear deletes a pure line and nothing else", func(t *testing.T) {
		if err := w.reg.SetAlias("d75885", "spare2"); err != nil {
			t.Fatal(err)
		}
		if err := w.reg.ClearAlias("d75884"); err != nil {
			t.Fatalf("ClearAlias: %v", err)
		}
		want := "# bench\ncam=1a2b3c@10.1.11.204:7777 model=esp32s3-dev\nspare2=d75885\n"
		if got := readConf(t, w.conf); !bytes.Equal(got, []byte(want)) {
			t.Fatalf("file = %q\n     want %q", got, want)
		}
		if got := aliasOf(w.reg, "d75884"); got != "" {
			t.Fatalf("alias after ClearAlias = %q, want none", got)
		}
		if _, err := w.reg.Resolve("left2"); err == nil {
			t.Errorf("Resolve(left2) still resolves after the clear")
		}
		w.sweep()
		if got := aliasOf(w.reg, "d75884"); got != "" {
			t.Errorf("a sweep resurrected the cleared alias: %q", got)
		}
		if got := aliasOf(w.rebuild(), "d75884"); got != "" {
			t.Errorf("a restart reads the cleared alias as %q", got)
		}
	})

	t.Run("clear refuses a line that carries a payload", func(t *testing.T) {
		before, existed := stampFile(t, w.conf)
		err := w.reg.ClearAlias("1a2b3c")
		if !errors.Is(err, ErrLineHasPayload) {
			t.Fatalf("ClearAlias(1a2b3c) = %v, want ErrLineHasPayload", err)
		}
		assertUnwritten(t, w.conf, before, existed)
		if got := aliasOf(w.reg, "1a2b3c"); got != "cam" {
			t.Errorf("a refused clear dropped the alias: %q", got)
		}
	})

	t.Run("clearing a board that was never aliased writes nothing", func(t *testing.T) {
		before, existed := stampFile(t, w.conf)
		if err := w.reg.ClearAlias("0aa001"); err != nil {
			t.Fatalf("ClearAlias(never aliased): %v", err)
		}
		assertUnwritten(t, w.conf, before, existed)
	})

	// An alias may be written for a board no discovery face has seen
	// yet — the line lands, nothing materializes (a never-seen board
	// is not a known board), and the alias is waiting the day it is.
	t.Run("an alias written ahead of discovery is waiting when the board appears", func(t *testing.T) {
		if err := w.reg.SetAlias("0badf0", "newcomer"); err != nil {
			t.Fatalf("SetAlias(never seen): %v", err)
		}
		if knownTo(w.reg, "0badf0") {
			t.Fatalf("an alias for a never-seen board materialized an entry")
		}
		w.boards = append(w.boards, worldBoard("0badf0", "xuansi-0badf0.local"))
		w.sweep()
		if got := aliasOf(w.reg, "0badf0"); got != "newcomer" {
			t.Fatalf("alias when the board appears = %q, want newcomer", got)
		}
		if b, err := w.reg.Resolve("newcomer"); err != nil || b.MAC6 != "0badf0" {
			t.Errorf("Resolve(newcomer) = %v, %v", b.MAC6, err)
		}
	})

	// the world and the file agree after all of the above
	t.Run("memory is what a restart reads", func(t *testing.T) {
		fresh := w.rebuild()
		for _, b := range w.reg.Snapshot() {
			if got, want := b.Alias, aliasOf(fresh, b.MAC6); got != want {
				t.Errorf("board %s: alias in memory %q, after a restart %q", b.MAC6, got, want)
			}
		}
	})
}

// TestSetAliasNoopIsDecidedFromTheFile: "already has this alias" is
// the FILE's verdict, never the memory's. A hand edit while the daemon
// runs leaves the two apart; a set must then bring the file — and so
// the next start — to what was asked for, not answer success over a
// stale memory. After every successful call memory equals a restart.
func TestSetAliasNoopIsDecidedFromTheFile(t *testing.T) {
	captureLogs(t)
	w := newAliasWorld(t, "bench=d75884\n", worldBoard("d75884", "xuansi-d75884.local"))
	handEdit := func(content string) {
		t.Helper()
		if err := os.WriteFile(w.conf, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	// a hand edit: the file now says "left", the memory still "bench".
	// Asking for the alias the daemon ALREADY holds is not a no-op —
	// the file's line is renamed to it.
	handEdit("left=d75884\n")
	if err := w.reg.SetAlias("d75884", "bench"); err != nil {
		t.Fatalf("SetAlias(d75884, bench): %v", err)
	}
	if got := readConf(t, w.conf); !bytes.Equal(got, []byte("bench=d75884\n")) {
		t.Fatalf("after SetAlias(d75884, \"bench\") the file = %q — the call answered over a stale memory (memory %q, a restart would read %q)",
			got, aliasOf(w.reg, "d75884"), aliasOf(w.rebuild(), "d75884"))
	}

	// they agree again, and the same call is a true no-op: nothing is written
	before, existed := stampFile(t, w.conf)
	if err := w.reg.SetAlias("d75884", "bench"); err != nil {
		t.Fatalf("SetAlias(current): %v", err)
	}
	assertUnwritten(t, w.conf, before, existed)

	// a line someone removed by hand comes back the same way
	handEdit("# the bench line was removed by hand\n")
	if err := w.reg.SetAlias("d75884", "bench"); err != nil {
		t.Fatalf("SetAlias(d75884, bench) after the line was removed by hand: %v", err)
	}
	if got := readConf(t, w.conf); !bytes.Equal(got, []byte("# the bench line was removed by hand\nbench=d75884\n")) {
		t.Fatalf("file = %q, want the alias line back after the last complete line", got)
	}

	// a hand edit that moved the alias to another name: asking for THAT
	// name is a no-op on the file, and the memory follows the file
	handEdit("left=d75884\n")
	if got := aliasOf(w.reg, "d75884"); got != "bench" {
		t.Fatalf("precondition: memory still holds %q, want bench", got)
	}
	before, existed = stampFile(t, w.conf)
	if err := w.reg.SetAlias("d75884", "left"); err != nil {
		t.Fatalf("SetAlias(d75884, left): %v", err)
	}
	assertUnwritten(t, w.conf, before, existed)
	if got, want := aliasOf(w.reg, "d75884"), aliasOf(w.rebuild(), "d75884"); got != want {
		t.Fatalf("memory %q, a restart %q — they must agree after a successful call", got, want)
	}

	// the same holds for a clear: memory is whatever the file says
	if err := w.reg.ClearAlias("d75884"); err != nil {
		t.Fatalf("ClearAlias: %v", err)
	}
	if got, want := aliasOf(w.reg, "d75884"), aliasOf(w.rebuild(), "d75884"); got != "" || want != "" {
		t.Fatalf("after the clear: memory %q, a restart %q, want neither", got, want)
	}

	// The name a board already holds is the status quo, not a new claim:
	// even in the degenerate world of a hand-written file that gives two
	// boards the same name, re-asking for a board's own alias is the
	// no-op success it always was — while claiming that name for the
	// OTHER board is still the collision it is.
	t.Run("a board's own alias is never re-judged as a collision", func(t *testing.T) {
		dup := newAliasWorld(t, "a=d75884\na=1a2b3c\n", worldBoard("d75884", "xuansi-d75884.local"), worldBoard("1a2b3c", "xuansi-1a2b3c.local"))
		before, existed := stampFile(t, dup.conf)
		if err := dup.reg.SetAlias("d75884", "a"); err != nil {
			t.Fatalf("SetAlias(d75884, its own alias a): %v, want the no-op success", err)
		}
		assertUnwritten(t, dup.conf, before, existed)
		if err := dup.reg.SetAlias("d75885", "a"); err == nil || !strings.Contains(err.Error(), "is already board 1a2b3c's alias") {
			t.Fatalf("SetAlias(d75885, a) = %v, want the collision with 1a2b3c", err)
		}
	})
}

// TestSetAliasOwnIdentityIsNotACollision: uniqueness is Resolve's
// rule — an alias is a selector, so it must name exactly one board.
// The target's own mac6 and hostname never count: an alias that also
// prefix-matches its OWN board resolves to that one board either way.
func TestSetAliasOwnIdentityIsNotACollision(t *testing.T) {
	w := newAliasWorld(t, "", worldBoard("d75885", "xuansi-d75885.local"))
	for _, alias := range []string{"d75885", "d758", "xuansi-d75885", "xuansi-d7"} {
		if err := w.reg.SetAlias("d75885", alias); err != nil {
			t.Fatalf("SetAlias(d75885, %q): %v, want accepted (it resolves to d75885 alone)", alias, err)
		}
		if b, err := w.reg.Resolve(alias); err != nil || b.MAC6 != "d75885" {
			t.Errorf("Resolve(%q) = %v, %v; want exactly d75885", alias, b.MAC6, err)
		}
	}
}

// TestSetAliasUniquenessIsAgainstTheWorldNow: the honest limit of a
// write-time check. An alias that is free today may collide with a
// board discovered tomorrow — that collision is not silently resolved
// and not silently ignored: it surfaces through Resolve's existing
// ambiguity error, and clearing or renaming the alias clears it.
func TestSetAliasUniquenessIsAgainstTheWorldNow(t *testing.T) {
	w := newAliasWorld(t, "", worldBoard("d75885", "xuansi-d75885.local"))
	if err := w.reg.SetAlias("d75885", "newbie"); err != nil {
		t.Fatalf("SetAlias: %v", err)
	}
	// a board whose hostname starts with the alias appears later
	w.boards = append(w.boards, worldBoard("0badf0", "newbie.local"))
	w.sweep()
	_, err := w.reg.Resolve("newbie")
	if err == nil || !strings.Contains(err.Error(), "ambiguous") {
		t.Fatalf("Resolve(newbie) after the collision = %v, want the ambiguity error", err)
	}
	if b, err := w.reg.Resolve("d75885"); err != nil || b.MAC6 != "d75885" {
		t.Fatalf("the mac6 still resolves: %v, %v", b.MAC6, err)
	}
	// clearing the alias ends the ambiguity (the line is pure)
	if err := w.reg.ClearAlias("d75885"); err != nil {
		t.Fatalf("ClearAlias: %v", err)
	}
	if b, err := w.reg.Resolve("newbie"); err != nil || b.MAC6 != "0badf0" {
		t.Errorf("Resolve(newbie) after the clear = %v, %v; want exactly 0badf0", b.MAC6, err)
	}
}

// TestSetAliasConcurrent: the alias writers are serialized — one
// validate → edit → sync at a time. The test stands inside the file
// edit (the editAlias seam) and flags any overlap: without the
// writer lock, writers that all passed validation would read the same
// file and the last rename would silently drop the others' lines.
// With it, every accepted alias lands in the file AND in memory.
func TestSetAliasConcurrent(t *testing.T) {
	w := newAliasWorld(t, "")
	orig := editAlias
	var inside, peak atomic.Int32
	editAlias = func(path, mac6, alias string, clear bool) error {
		if n := inside.Add(1); n > 1 {
			peak.Store(n)
		}
		time.Sleep(2 * time.Millisecond) // hold the write window open
		defer inside.Add(-1)
		return orig(path, mac6, alias, clear)
	}
	t.Cleanup(func() { editAlias = orig })

	const n = 16
	start := make(chan struct{})
	done := make(chan error, n)
	for i := 0; i < n; i++ {
		go func(i int) {
			<-start
			done <- w.reg.SetAlias(fmt.Sprintf("c%05x", i), fmt.Sprintf("board-%02d", i))
		}(i)
	}
	close(start)
	for i := 0; i < n; i++ {
		if err := <-done; err != nil {
			t.Fatalf("SetAlias: %v", err)
		}
	}
	if p := peak.Load(); p > 1 {
		t.Fatalf("%d alias writes were inside the file edit at once — the writers are not serialized", p)
	}
	aliases, _, _ := parseConf(t, w.conf)
	if len(aliases) != n {
		t.Fatalf("the file holds %d aliases, want %d (a lost update)", len(aliases), n)
	}
	for i := 0; i < n; i++ {
		if got := aliases[fmt.Sprintf("c%05x", i)]; got != fmt.Sprintf("board-%02d", i) {
			t.Fatalf("aliases = %#v", aliases)
		}
	}
	// concurrent clears ride the same serialization
	done = make(chan error, n)
	for i := 0; i < n; i++ {
		go func(i int) {
			done <- w.reg.ClearAlias(fmt.Sprintf("c%05x", i))
		}(i)
	}
	for i := 0; i < n; i++ {
		if err := <-done; err != nil {
			t.Fatalf("ClearAlias: %v", err)
		}
	}
	if p := peak.Load(); p > 1 {
		t.Fatalf("%d alias writes overlapped across the clears", p)
	}
	if aliases, _, _ := parseConf(t, w.conf); len(aliases) != 0 {
		t.Fatalf("aliases left after the concurrent clears: %#v", aliases)
	}
	assertNoLeftovers(t, w.dir)
}

// TestSetAliasWithoutBoardsFile: a registry that was never told where
// its file lives (the degenerate host where even the default path
// cannot be resolved) refuses honestly instead of pretending.
func TestSetAliasWithoutBoardsFile(t *testing.T) {
	r := NewRegistry(nil, nil)
	if r.BoardsFile() != "" {
		t.Fatalf("BoardsFile() = %q, want empty", r.BoardsFile())
	}
	for name, call := range map[string]func() error{
		"SetAlias":   func() error { return r.SetAlias("d75884", "bench") },
		"ClearAlias": func() error { return r.ClearAlias("d75884") },
	} {
		err := call()
		if err == nil || !strings.Contains(err.Error(), "--boards-file") {
			t.Errorf("%s without a boards file = %v, want a refusal naming --boards-file", name, err)
		}
	}
}

func TestIsAlias(t *testing.T) {
	for _, tc := range []struct {
		alias string
		want  bool
	}{
		{"a", true}, {"0", true}, {"bench", true}, {"left-bench", true}, {"a_b", true}, {"a-b_c1", true},
		{strings.Repeat("b", 32), true},
		{strings.Repeat("b", 33), false}, {"", false}, {"Bench", false}, {"bench1A", false},
		{"_bench", false}, {"-bench", false}, {"bench 1", false}, {" bench", false}, {"bench ", false},
		{"bench.1", false}, {"bench/1", false}, {"a=b", false}, {"板", false}, {"a\tb", false},
	} {
		if got := isAlias(tc.alias); got != tc.want {
			t.Errorf("isAlias(%q) = %v, want %v", tc.alias, got, tc.want)
		}
	}
}

// ---------------------------------------------------------------------------
// the line editor, generated: every line shape × both operations
// ---------------------------------------------------------------------------

// phys is one physical line for the generator's oracle: its bytes
// without the terminator, and the terminator itself.
type phys struct{ text, eol string }

// splitPhys is the test's own view of a file's lines (it shares
// nothing with the editor's line model).
func splitPhys(data []byte) []phys {
	var out []phys
	s := string(data)
	for len(s) > 0 {
		i := strings.IndexByte(s, '\n')
		if i < 0 {
			return append(out, phys{s, ""})
		}
		text := s[:i]
		if strings.HasSuffix(text, "\r") {
			out = append(out, phys{strings.TrimSuffix(text, "\r"), "\r\n"})
		} else {
			out = append(out, phys{text, "\n"})
		}
		s = s[i+1:]
	}
	return out
}

// lineFact is what ONE boards.conf line is, stated from the rule
// itself: which board it names and under what name (the parser's
// per-line read), and whether it is PURE — `name=mac6` and nothing
// else: no @endpoint, no key=value tail, no comment, no stray word.
// (An inline comment is read with the test's own eye, inlineCommentAt:
// it leaves the head and the token list, so nothing in them would
// betray it — yet the line is not pure.)
func lineFact(line string) (mac6, name string, names, pure bool) {
	t := strings.TrimSpace(line)
	if t == "" || strings.HasPrefix(t, "#") {
		return
	}
	head, tokens := splitTokenTail(t)
	h := parseHead(head)
	if !h.ok {
		return
	}
	_, after, _ := strings.Cut(head, "=")
	mac6, name, names = h.mac6, h.name, true
	pure = !h.hasEndpoint && len(tokens) == 0 && inlineCommentAt(t) < 0 && strings.EqualFold(strings.TrimSpace(after), h.mac6)
	return
}

// TestEditAliasRandomized: the charter, stated as a property and
// checked over generated files. For every generated boards.conf and
// every operation on it:
//
//   - SET renames exactly the LAST line naming the board (the line
//     the parser lets win) and touches no other line — or appends one
//     line when no line names it, every original byte staying in
//     place (a dangling unterminated last line stays last); when the
//     winner already carries the alias, nothing is written;
//   - CLEAR deletes the line only when it is the ONE line naming the
//     board AND pure; a payload line or a second line naming the board
//     is refused with ErrLineHasPayload and the file stays whole —
//     unless every line naming the board already carries the mac6 as
//     its name (the safe recipe, done): no alias is left to clear, the
//     call succeeds and nothing is written;
//   - either way the production parser reads the edited file as the
//     original world plus exactly the intended change.
//
// The generator spans the three line shapes with an inline comment on
// each (` # plain note`, a note riding a token tail, a note with an =
// inside), so the ruled comment grammar is under the property too —
// and three of its names ARE target mac6s, so files name a board by
// its own mac6 often enough to put the end of the safe recipe under
// the property (both counts are asserted at the end, not assumed).
func TestEditAliasRandomized(t *testing.T) {
	captureLogs(t) // generated malformed lines warn by design
	rng := rand.New(rand.NewSource(20261123))
	pick := func(from ...string) string { return from[rng.Intn(len(from))] }
	newLine := func() string {
		switch rng.Intn(12) {
		case 0:
			return pick("", " ", "   ", "\t", " \t ")
		case 1:
			return pick("", " ", "\t") + "#" + pick(" bench boards", " d75884 sits on the bench", "", " note: model= is per board", " d75884=d75884")
		case 2:
			return pick("stray line", "=d75884", "ghost=abc", "no-separator", "==d75884", "d75884", "#",
				"bench=d75884 stray", "note=d75884 stray # plain note", "bench = d75884 stray")
		}
		ws := func() string { return pick("", " ", "  ", "\t") }
		return pick("", " ", "\t") +
			pick("bench", "cam", "a-b", "x_y", "two words", "lab", "d75884", "d75884", "1a2b3c", "abcdef", "xuansi", "note") +
			ws() + "=" + ws() +
			pick("d75884", "D75884", "1a2b3c", "abcdef", "0badf0", "0BADF0") +
			pick("", " ", "  ") +
			pick("", "@10.1.11.204:7777", "@[fe80::1]:7777", "@", " model=m1", " model=m1 # bay note", " foo=bar",
				" @10.1.11.204:7777", " model=m1 model=m2", " # plain note", " stray", " model=", " model=m1\t",
				" model=m1 # note with = inside") +
			pick("", " ", "  ", "\t")
	}

	// genCase is one generated boards.conf plus the oracle's own view
	// of it: its physical lines (the generator's — shared with nothing
	// in the editor), what each line naming the target is, and the
	// production parser's verdict on the original.
	type fact struct {
		name string
		pure bool
	}
	type genCase struct {
		iter            int
		mac6            string
		path            string
		before          []byte
		lines           []phys
		facts           []fact // per line; set only where the line names the target
		naming          []int
		aliases, models map[string]string
		pins            map[string]Pin
	}
	where := func(c *genCase, op string) string {
		return fmt.Sprintf("iteration %d: %s %s\n file %q\n lines naming it: %v (facts %+v)",
			c.iter, op, c.mac6, c.before, c.naming, c.facts)
	}
	clone := func(m map[string]string) map[string]string {
		out := make(map[string]string, len(m))
		for k, v := range m {
			out[k] = v
		}
		return out
	}
	// semantic: the parser reads the edited file as the original world
	// with exactly the one intended alias change applied.
	semantic := func(t *testing.T, c *genCase, wantAliases map[string]string) {
		t.Helper()
		aAliases, aPins, aModels := parseConf(t, c.path)
		if !reflect.DeepEqual(aAliases, wantAliases) {
			t.Fatalf("parser: aliases = %#v, want %#v\n%s", aAliases, wantAliases, where(c, "->"))
		}
		if !reflect.DeepEqual(aPins, c.pins) || !reflect.DeepEqual(aModels, c.models) {
			t.Fatalf("parser: pins/models = %#v %#v, want the original's %#v %#v\n%s",
				aPins, aModels, c.pins, c.models, where(c, "->"))
		}
	}
	oracleSet := func(t *testing.T, c *genCase, alias string, after []byte, err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("EditAlias(set %q): %v\n%s", alias, err, where(c, "set"))
		}
		want := clone(c.aliases)
		want[c.mac6] = alias
		semantic(t, c, want)
		al := splitPhys(after)
		last := -1
		if len(c.naming) > 0 {
			last = c.naming[len(c.naming)-1]
		}
		switch {
		case last >= 0 && c.facts[last].name == alias:
			// the winner already carries the alias: not a byte is written
			if !bytes.Equal(after, c.before) {
				t.Fatalf("a no-op set rewrote the file:\n before %q\n after  %q\n%s", c.before, after, where(c, "set"))
			}
		case last >= 0:
			// rename: the same line count, every other line
			// byte-identical (terminator included), the winner
			// differing in its name only
			if len(al) != len(c.lines) {
				t.Fatalf("line count %d -> %d\n%s", len(c.lines), len(al), where(c, "set"))
			}
			for i := range c.lines {
				if i != last && al[i] != c.lines[i] {
					t.Fatalf("line %d moved: %#v -> %#v\n%s", i, c.lines[i], al[i], where(c, "set"))
				}
			}
			text, name := c.lines[last].text, c.facts[last].name
			lead := len(text) - len(strings.TrimLeftFunc(text, unicode.IsSpace))
			if text[lead:lead+len(name)] != name {
				t.Fatalf("oracle: %q does not carry the name %q where expected\n%s", text, name, where(c, "set"))
			}
			if got, wantLine := al[last], text[:lead]+alias+text[lead+len(name):]; got.text != wantLine || got.eol != c.lines[last].eol {
				t.Fatalf("target line = %#v, want {%q %q}\n%s", got, wantLine, c.lines[last].eol, where(c, "set"))
			}
		default:
			// append: one new line `alias=mac6` after the file's last
			// COMPLETE line — a dangling unterminated last line stays
			// last and unterminated — and every original line is
			// byte-identical, terminator included
			if len(al) != len(c.lines)+1 {
				t.Fatalf("line count %d -> %d, want +1\n%s", len(c.lines), len(al), where(c, "set"))
			}
			eol := "\n" // the convention in effect at the file's end
			for _, l := range c.lines {
				if l.eol != "" {
					eol = l.eol
				}
			}
			at := len(c.lines)
			if n := len(c.lines) - 1; n >= 0 && c.lines[n].eol == "" {
				at = n
			}
			j := 0
			for i := range al {
				if i == at {
					if got := al[i]; got != (phys{alias + "=" + c.mac6, eol}) {
						t.Fatalf("appended line = %#v, want {%q %q}\n%s", got, alias+"="+c.mac6, eol, where(c, "set"))
					}
					continue
				}
				if al[i] != c.lines[j] {
					t.Fatalf("line %d moved: %#v -> %#v\n%s", j, c.lines[j], al[i], where(c, "set"))
				}
				j++
			}
		}
	}
	// mac6Named: every line naming the board already carries the mac6 as
	// its name, pure or not. (One PURE line named by the mac6 is still
	// deleted — the oracle matches that case first, as the editor does.)
	mac6Named := func(c *genCase) bool {
		for _, i := range c.naming {
			if c.facts[i].name != c.mac6 {
				return false
			}
		}
		return len(c.naming) > 0
	}
	mac6NamedClears, inlineCommentLines := 0, 0
	oracleClear := func(t *testing.T, c *genCase, after []byte, err error) {
		t.Helper()
		switch {
		case len(c.naming) == 1 && c.facts[c.naming[0]].pure:
			// the one line naming the board is pure: it goes, whole,
			// and nothing else moves
			if err != nil {
				t.Fatalf("EditAlias(clear): %v\n%s", err, where(c, "clear"))
			}
			gone := c.naming[0]
			al := splitPhys(after)
			if len(al) != len(c.lines)-1 {
				t.Fatalf("line count %d -> %d\n%s", len(c.lines), len(al), where(c, "clear"))
			}
			j := 0
			for i := range c.lines {
				if i != gone {
					if al[j] != c.lines[i] {
						t.Fatalf("line %d moved: %#v -> %#v\n%s", i, c.lines[i], al[j], where(c, "clear"))
					}
					j++
				}
			}
			want := clone(c.aliases)
			delete(want, c.mac6)
			semantic(t, c, want)
		case len(c.naming) == 0:
			// nothing names the board: nothing to clear, nothing written
			if err != nil || !bytes.Equal(after, c.before) {
				t.Fatalf("clear with no line: err = %v, file %q -> %q\n%s", err, c.before, after, where(c, "clear"))
			}
		case mac6Named(c):
			// every line naming the board already carries the mac6 as
			// its name — the safe recipe, done by the alias tool or by
			// hand: no alias is left to clear. Success, nothing written
			// (a single PURE line named by the mac6 never gets here: it
			// was deleted one case up).
			if err != nil || !bytes.Equal(after, c.before) {
				t.Fatalf("clear with every naming line already named by the mac6: err = %v, file %q -> %q\n%s",
					err, c.before, after, where(c, "clear"))
			}
			mac6NamedClears++
		default:
			// a payload line, or a second line naming the board: refused
			if !errors.Is(err, ErrLineHasPayload) || !bytes.Equal(after, c.before) {
				t.Fatalf("clear with %d naming line(s): err = %v, file %q -> %q\n%s",
					len(c.naming), err, c.before, after, where(c, "clear"))
			}
		}
	}

	const iterations = 1000
	for it := 0; it < iterations; it++ {
		dir := t.TempDir()
		c := &genCase{
			iter: it,
			path: filepath.Join(dir, "boards.conf"),
			mac6: pick("d75884", "1a2b3c", "abcdef", "0badf0", "0aa001"), // 0aa001 is in no generated line: always an append
		}
		n := 1 + rng.Intn(10)
		var b strings.Builder
		for i := 0; i < n; i++ {
			l := phys{newLine(), pick("\n", "\r\n")}
			if i == n-1 && rng.Intn(2) == 0 {
				l.eol = ""
			}
			c.lines = append(c.lines, l)
		}
		// an unterminated empty last line is not a line — it is the
		// end of the file
		for len(c.lines) > 0 && c.lines[len(c.lines)-1] == (phys{}) {
			c.lines = c.lines[:len(c.lines)-1]
		}
		for _, l := range c.lines {
			b.WriteString(l.text)
			b.WriteString(l.eol)
		}
		if err := os.WriteFile(c.path, []byte(b.String()), 0o644); err != nil {
			t.Fatal(err)
		}
		c.before = readConf(t, c.path)
		c.aliases, c.pins, c.models = parseConf(t, c.path)
		c.facts = make([]fact, len(c.lines))
		for i, l := range c.lines {
			if mac6, name, names, pure := lineFact(l.text); names && mac6 == c.mac6 {
				c.facts[i] = fact{name: name, pure: pure}
				c.naming = append(c.naming, i)
				if inlineCommentAt(l.text) >= 0 {
					inlineCommentLines++
				}
			}
		}

		if rng.Intn(2) == 0 {
			alias := pick("bench", "cam", "left-bench", "a_b", "n1", "x", "d75884", "xuansi-d7")
			err := EditAlias(c.path, c.mac6, alias, false)
			oracleSet(t, c, alias, readConf(t, c.path), err)
		} else {
			err := EditAlias(c.path, c.mac6, "", true)
			oracleClear(t, c, readConf(t, c.path), err)
		}
		assertNoLeftovers(t, dir)
	}
	// the two properties the corpus must actually have exercised: lines
	// already named by the mac6 (the end of the safe recipe), and the
	// target's line carrying an inline comment on some shape
	if mac6NamedClears == 0 {
		t.Errorf("no generated case reached the end of the safe recipe (every naming line already named by the mac6) — that branch of the oracle ran 0 times")
	}
	if inlineCommentLines == 0 {
		t.Errorf("no generated file named the target on a line with an inline comment — the ruled comment grammar ran 0 times under the property")
	}
	t.Logf("iterations %d: clears on lines already named by the mac6 %d; target lines carrying an inline comment %d", iterations, mac6NamedClears, inlineCommentLines)
}
