package discover

// boardsfile.go — the boards.conf LINE EDITOR: the only way the daemon
// writes the file (the alias write face). boards.conf is
// human-writable and daemon-writable at once, and the daemon's
// charter is narrow: it touches ONLY the name field of the target
// board's line — or appends or deletes a pure alias line. Every other
// byte of the file stays where it was: comments, blank lines, unknown
// and malformed lines, @endpoint pins, key=value tokens, an inline
// note, trailing whitespace, each line's own terminator (CRLF
// included) and a missing final newline. That is the invariant the
// tests pin byte for byte.
//
// The editor knows nothing about which aliases are legal or free —
// charset and uniqueness are validated one layer up (SetAlias),
// against the world. What it does guard is its own output: it refuses
// to write a line the parser could not read back.
//
// It reads the file exactly the way LoadBoardsFile does. Both ride
// splitTokenTail and parseHead, so "the line that names mac6" for the
// editor is by construction the line whose alias the parser registers.

import (
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode"
)

// replaceBoardsFile renames the new file over the old one and
// fsyncBoardsDir flushes the directory after it; the bodies are per-OS
// (boardsfile_unix.go / boardsfile_windows.go). Package vars so the
// tests can make either step fail.
var (
	replaceBoardsFile = replaceBoardsFileOS
	fsyncBoardsDir    = fsyncBoardsDirOS
)

// ErrLineHasPayload is the unalias refusal: clearing the alias would
// take more than the alias with it. Two shapes ride it — the board's
// line carries an @endpoint, key=value tokens or a note beside the
// name; or boards.conf names the board on more than one line (remove
// the winner, and a shadowed line resurfaces at the next start).
//
// The refusal always names the SAFE edit, never the one that destroys
// the payload. A boards.conf line needs its name: without it the whole
// line — @endpoint and model= included — parses to nothing, and is
// lost at the next start with only a warning in the log. So an alias
// is cleared off a payload line by RENAMING the name to the board's own
// mac6 (by hand, or as SetAlias(mac6, mac6), which the registry does
// through this editor): the payload stays, and the board answers to its
// mac6 exactly as an unaliased board does.
//
// That edit is the END of the refusal. A line (or several) already
// named by the mac6 carries no alias, and a clear on it is a success
// that writes nothing — never this error again with a coach that
// sends the reader where they already are.
var ErrLineHasPayload = errors.New("clearing the alias would take more than the alias with it")

// fileAlias is the name the boards.conf at path gives mac6 — the alias
// a daemon start would read for it — or "" when no line names the
// board. It rides nameSpan, so it is the parser's own verdict, read
// off the file as it is NOW (a file that cannot be read names nobody).
func fileAlias(path, mac6 string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	mac6 = strings.ToLower(strings.TrimSpace(mac6))
	name := ""
	for _, l := range splitConfLines(data) {
		if start, end, m, ok := nameSpan(l.text); ok && m == mac6 {
			name = l.text[start:end] // the LAST line naming the board, as the parser lets win
		}
	}
	return name
}

// confLine is one physical line of the file: its bytes without the
// terminator, and the terminator itself — "\n", "\r\n", or "" for an
// unterminated last line. Keeping the two apart is what lets an edit
// move one line's name while every other byte, the terminators and a
// missing final newline included, lands back exactly where it was.
type confLine struct {
	text string
	eol  string
}

// splitConfLines cuts data into its physical lines; joinConfLines is
// its exact inverse for every input (an empty file is no lines).
func splitConfLines(data []byte) []confLine {
	var lines []confLine
	for len(data) > 0 {
		i := bytes.IndexByte(data, '\n')
		if i < 0 {
			return append(lines, confLine{text: string(data)})
		}
		text, eol := data[:i], "\n"
		if len(text) > 0 && text[len(text)-1] == '\r' {
			text, eol = text[:len(text)-1], "\r\n"
		}
		lines = append(lines, confLine{text: string(text), eol: eol})
		data = data[i+1:]
	}
	return lines
}

// joinConfLines is splitConfLines' inverse.
func joinConfLines(lines []confLine) []byte {
	var b strings.Builder
	for _, l := range lines {
		b.WriteString(l.text)
		b.WriteString(l.eol)
	}
	return []byte(b.String())
}

// nameSpan locates, inside ONE raw line (terminator off), the alias
// the parser would register: the byte range of the name, [start,end),
// and the board the line names. ok is false for every line
// LoadBoardsFile skips — blank, comment, malformed. The span is the
// parser's own name, trimmed exactly as parseHead trims it, so what
// the editor replaces is precisely the field the parser reads — and
// nothing a human wrote around it.
func nameSpan(raw string) (start, end int, mac6 string, ok bool) {
	t := strings.TrimSpace(raw)
	if t == "" || strings.HasPrefix(t, "#") {
		return
	}
	head, _ := splitTokenTail(t)
	h := parseHead(head)
	if !h.ok {
		return
	}
	start = len(raw) - len(strings.TrimLeftFunc(raw, unicode.IsSpace))
	return start, start + len(h.name), h.mac6, true
}

// isPureAlias reports whether raw is a pure alias line — `name=mac6`
// and NOTHING else: no @endpoint, no key=value tail, no comment, no
// stray word. The parser's tolerated spellings are still pure (spaces
// around the "=", an uppercase mac6, trailing whitespace): deleting
// the line takes the alias and nothing a human wrote beside it. An
// inline comment leaves the head and the token list untouched — so
// nothing in them betrays it — yet it is something a human wrote
// beside the alias: the line is read with commentStart too, and is not
// pure.
func isPureAlias(raw string) bool {
	t := strings.TrimSpace(raw)
	if t == "" || strings.HasPrefix(t, "#") || commentStart(t) >= 0 {
		return false
	}
	head, tokens := splitTokenTail(t)
	h := parseHead(head)
	if !h.ok || h.hasEndpoint || len(tokens) != 0 {
		return false
	}
	_, value, _ := strings.Cut(head, "=")
	return strings.ToLower(strings.TrimSpace(value)) == h.mac6
}

// mac6Named reports whether every line at (indexes into lines) names
// its board by the board's own mac6 — exactly, as the editor would
// write it (an uppercase spelling is not there yet: its coach still
// leads somewhere).
func mac6Named(lines []confLine, at []int, mac6 string) bool {
	for _, i := range at {
		start, end, _, ok := nameSpan(lines[i].text)
		if !ok || lines[i].text[start:end] != mac6 {
			return false
		}
	}
	return true
}

// eolOf is the line terminator a NEW line takes after lines: the one
// the file's last terminator uses (a CRLF file stays CRLF); "\n" for
// an empty or terminator-less file.
func eolOf(lines []confLine) string {
	eol := "\n"
	for _, l := range lines {
		if l.eol != "" {
			eol = l.eol
		}
	}
	return eol
}

// EditAlias is the editor's one entry point: it renames, appends or
// deletes a board's alias line in the boards.conf at path — and
// touches nothing else in the file.
//
//   - clear=false (set): the LAST line naming mac6 — the one whose
//     alias the parser lets win — gets alias as its name, everything
//     else on the line staying byte for byte. When no line names mac6,
//     a new line `alias=mac6` is added after the file's last complete
//     line (right before a dangling unterminated last line, so the
//     file keeps ending — or not ending — with a newline exactly as it
//     did); a missing file is created (zero-config's first alias).
//     When the line already carries alias, nothing is written.
//   - clear=true: the line is deleted only when it is the ONE line
//     naming mac6 AND a pure alias line (`name=mac6`, nothing else).
//     Anything more is ErrLineHasPayload, and nothing is written —
//     with one end state excepted: a payload line (or several lines)
//     already named by the board's own mac6 has no alias left to
//     clear, and the clear is a success that writes nothing. When no
//     line names mac6 there is nothing to clear either: nothing is
//     written and no file is created.
//
// No existing byte ever changes value. Bytes leave the file only with
// the name field a rename replaces or the pure line a clear deletes;
// bytes enter it only as the name field and the one line a set adds.
func EditAlias(path, mac6, alias string, clear bool) error {
	if path == "" {
		return errors.New("discover: alias: no boards file path was given")
	}
	mac6 = strings.ToLower(strings.TrimSpace(mac6))
	if !isMac6(mac6) {
		return fmt.Errorf("discover: alias: %q is not a mac6 (6 lowercase hex chars)", mac6)
	}
	if !clear && !isAlias(alias) {
		return fmt.Errorf("discover: alias: %q is outside %s", alias, aliasCharset)
	}

	data, err := os.ReadFile(path)
	switch {
	case err == nil:
	case os.IsNotExist(err):
		// zero-config: no file yet — a set creates it, a clear has nothing to do
	default:
		return fmt.Errorf("discover: alias: read %s: %w", path, err)
	}
	lines := splitConfLines(data)

	// the target is the LAST line naming mac6 — the parser's winner;
	// any earlier one is shadowed and is not ours to touch
	target, earlier := -1, []int(nil)
	for i, l := range lines {
		if _, _, m, ok := nameSpan(l.text); ok && m == mac6 {
			if target >= 0 {
				earlier = append(earlier, target)
			}
			target = i
		}
	}

	if clear {
		switch {
		case target < 0:
			return nil
		case len(earlier) > 0:
			if mac6Named(lines, append(earlier, target), mac6) {
				// every line already named by the mac6 — the multi-line
				// coach, followed by hand: nothing is left to clear
				return nil
			}
			at := make([]string, len(earlier)+1)
			for i, n := range append(earlier, target) {
				at[i] = fmt.Sprint(n + 1)
			}
			return fmt.Errorf("discover: unalias %s: %w: %s names the board on %d lines (%s) — the daemon never splits its edit across several lines; to clear this alias, rename the name on each of them to the board's mac6 (%s) by hand",
				mac6, ErrLineHasPayload, path, len(at), strings.Join(at, ", "), mac6)
		case !isPureAlias(lines[target].text) && !mac6Named(lines, []int{target}, mac6):
			// the safe edit, spelled out: which name, to what, and that
			// the rest of the line stays (the seat's alias call does it)
			start, end, _, _ := nameSpan(lines[target].text)
			return fmt.Errorf("discover: unalias %s: %w: %s names it as %q — a boards.conf line needs its name (without it the @endpoint and model= on that line are lost at the next start), so the daemon deletes only a pure alias=mac6 line; to clear this alias, rename the name to the board's mac6 (here %q → %q, the rest of the line unchanged) — by hand, or by setting the alias to %s",
				mac6, ErrLineHasPayload, path, strings.TrimSpace(lines[target].text), lines[target].text[start:end], mac6, mac6)
		case !isPureAlias(lines[target].text):
			// the safe recipe, done — by the alias call or by hand: the
			// payload line is named by the mac6 and carries no alias.
			// Success, and not a byte written: refusing here again, from
			// the very place the coach sent the reader, was a terminal
			// loop (`"d75884" → "d75884"`). The line stays what it is —
			// a payload line, never one the daemon deletes.
			return nil
		}
		lines = append(lines[:target], lines[target+1:]...)
	} else if target >= 0 {
		start, end, _, _ := nameSpan(lines[target].text)
		if lines[target].text[start:end] == alias {
			return nil // the line already carries this name: not a byte is written
		}
		lines[target].text = lines[target].text[:start] + alias + lines[target].text[end:]
	} else {
		// The new line goes after the file's last COMPLETE line. When
		// the file ends without a newline that spot is right before the
		// dangling last line — the latest place a whole line fits
		// without moving a byte — so the file keeps ending, or not
		// ending, with a newline exactly as it did.
		eol := eolOf(lines)
		at := len(lines)
		if n := len(lines) - 1; n >= 0 && lines[n].eol == "" {
			at = n
		}
		lines = append(lines, confLine{})
		copy(lines[at+1:], lines[at:])
		lines[at] = confLine{text: alias + "=" + mac6, eol: eol}
	}
	if err := writeBoardsFile(path, joinConfLines(lines)); err != nil {
		return fmt.Errorf("discover: alias: %w", err)
	}
	return nil
}

// tempPattern is the writer's temp-file name, inside the target's own
// directory (the rename never crosses a filesystem).
const tempPattern = ".boards.conf-*.tmp"

// staleTempAge is how old a leftover temp file must be before a write
// sweeps it. A temp file lives for microseconds inside one write, and
// the alias writers are serialized, so anything this old is a crashed
// or killed write's litter; anything younger is left alone, so the
// sweep can never race a live one.
const staleTempAge = time.Minute

// writeBoardsFile lands data at path the way a careful editor saves —
// never a torn write, never a lost original, for the file's first
// write as much as for every later one:
//
//   - a temp file in the target's directory is written, fsynced and
//     closed, then renamed over the target (replaceBoardsFile; on
//     windows a bounded retry past a scanner's or an editor's
//     transient hold), and the directory fsynced best-effort. A
//     reader — the next daemon start, a human's editor — only ever
//     sees the old file or the new one, and a failure before the
//     rename leaves the original as it was (a file that did not exist
//     still absent).
//   - an EXISTING file gets its permission bits back exactly. A NEW
//     file (zero-config's first alias) takes the mode a plain 0644
//     create gives — the umask applies, as it does to anything else a
//     user's tools create — parent directories included.
//   - a symlinked boards.conf is followed, not replaced: the link
//     stays a link (a dangling one included) and its target is what
//     changes — a relative target resolved from the link's real
//     directory, the way the kernel reads it.
//   - a temp file a crashed write left behind is swept once it is
//     stale (sweepStaleTemps).
func writeBoardsFile(path string, data []byte) error {
	target, isNew := path, false
	info, err := os.Stat(path)
	switch {
	case err == nil:
		if resolved, rerr := filepath.EvalSymlinks(path); rerr == nil {
			target = resolved
		}
	case os.IsNotExist(err):
		isNew = true
		// a dangling symlink still says where the file belongs: it is
		// followed (a rename would replace the link with a plain file)
		if dest, rerr := os.Readlink(path); rerr == nil {
			if !filepath.IsAbs(dest) {
				// A relative target is read from the link's REAL directory:
				// the kernel resolves a `..` through the directories as
				// they are on disk, not through the path as spelled. A
				// lexical join onto the spelled path cancels a symlinked
				// config directory textually (`link/conf/../x` →
				// `link/x`) and lands the file where nothing that reads
				// the link would look — after creating that directory.
				// So the directory is resolved first, and a directory
				// that cannot be resolved refuses: nothing is written.
				dir, derr := filepath.EvalSymlinks(filepath.Dir(path))
				if derr != nil {
					return fmt.Errorf("resolve the directory of %s: %w — nothing was written", path, derr)
				}
				dest = filepath.Join(dir, dest)
			}
			target = dest
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return fmt.Errorf("create the directory for %s: %w", path, err)
		}
	default:
		return fmt.Errorf("stat %s: %w", path, err)
	}
	dir := filepath.Dir(target)
	sweepStaleTemps(dir)

	perm := os.FileMode(0o644) // a new file: what a plain create gives
	if !isNew {
		perm = info.Mode().Perm()
	}
	tmp, err := createBoardsTemp(dir, perm)
	if err != nil {
		return fmt.Errorf("write %s: %w (is its directory writable?)", path, err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // no-op after a successful rename
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("write %s: %w", path, err)
	}
	if !isNew {
		// the original's bits back EXACTLY: creation went through the
		// umask, which may have cleared one the original has
		if err := tmp.Chmod(perm); err != nil {
			tmp.Close()
			return fmt.Errorf("write %s: %w", path, err)
		}
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("write %s: %w", path, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	if err := replaceBoardsFile(tmpName, target); err != nil {
		return fmt.Errorf("replace %s: %w — the file is unchanged (is another program holding it open?)", path, err)
	}
	// The directory fsync is the crash-durability half — the rename
	// entry itself surviving power loss — and it is BEST-EFFORT: by
	// here the file is replaced and its data flushed, and a filesystem
	// that refuses a directory fsync must not turn a write that
	// happened into a reported failure: the caller would then keep its
	// memory out of step with a file that did change.
	if err := fsyncBoardsDir(dir); err != nil {
		slog.Warn("discover: boards.conf was written, but its directory could not be fsynced (the rename may not survive a crash)",
			"dir", dir, "err", err)
	}
	return nil
}

// createBoardsTemp creates a uniquely named temp file in dir, open for
// writing, with perm — the kernel applies the umask exactly as it does
// to any other create, which is the one thing os.CreateTemp (a fixed
// 0600) cannot offer.
func createBoardsTemp(dir string, perm os.FileMode) (*os.File, error) {
	for try := 0; try < 10000; try++ {
		f, err := os.OpenFile(filepath.Join(dir, ".boards.conf-"+rand.Text()+".tmp"),
			os.O_RDWR|os.O_CREATE|os.O_EXCL, perm)
		if err == nil {
			return f, nil
		}
		if !errors.Is(err, fs.ErrExist) {
			return nil, err
		}
	}
	return nil, fs.ErrExist
}

// sweepStaleTemps removes the temp files a crashed or killed write left
// in dir — best-effort, failures ignored: litter is harmless, a write
// is not.
func sweepStaleTemps(dir string) {
	stale := time.Now().Add(-staleTempAge)
	matches, _ := filepath.Glob(filepath.Join(dir, tempPattern))
	for _, m := range matches {
		if info, err := os.Stat(m); err == nil && info.Mode().IsRegular() && info.ModTime().Before(stale) {
			os.Remove(m)
		}
	}
}
