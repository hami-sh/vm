package web

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/hami-sh/vm/internal/api"
	"github.com/hami-sh/vm/internal/web/recording"
)

// The Runs endpoints only read what a run recorded under Options.RunsDir.
// Nothing is re-run, so playback needs no determinism.

func (s *Server) listRuns(w http.ResponseWriter, _ *http.Request) {
	runs, err := recording.List(s.o.RunsDir)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, runs)
}

func runErr(err error) error {
	if errors.Is(err, recording.ErrNotFound) {
		return &api.Error{Status: http.StatusNotFound, Msg: err.Error()}
	}
	return err
}

func (s *Server) getRun(w http.ResponseWriter, r *http.Request) {
	rep, err := recording.Report(s.o.RunsDir, r.PathValue("id"))
	if err != nil {
		writeError(w, runErr(err))
		return
	}
	writeJSON(w, http.StatusOK, rep)
}

// maxLine bounds one snapshots.jsonl line (a snapshot is about 10 KB).
const maxLine = 16 << 20

// runSnapshots streams snapshots.jsonl as NDJSON, gzipped when the client
// accepts it. A long run has thousands of 100 ms snapshots, so the Runs page
// asks for fewer: ?every=N keeps one line in N, and ?max=M picks the smallest
// N that returns about M lines. See thinSnapshots for what is kept.
func (s *Server) runSnapshots(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	every, err := positive(q.Get("every"), 1)
	if err != nil {
		writeError(w, badRequest("every must be a positive integer"))
		return
	}
	limit, err := positive(q.Get("max"), 0)
	if err != nil {
		writeError(w, badRequest("max must be a positive integer"))
		return
	}
	f, err := recording.OpenSnapshots(s.o.RunsDir, r.PathValue("id"))
	if err != nil {
		writeError(w, runErr(err))
		return
	}
	defer f.Close()
	if limit > 0 {
		n, err := countLines(f)
		if err != nil {
			writeError(w, err)
			return
		}
		every = max(every, (n+limit-1)/limit)
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Vary", "Accept-Encoding")
	var out io.Writer = w
	if strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
		w.Header().Set("Content-Encoding", "gzip")
		gz := gzip.NewWriter(w)
		defer gz.Close()
		out = gz
	}
	if err := thinSnapshots(out, f, every); err != nil {
		s.o.Logf("web: run %s snapshots: %v", r.PathValue("id"), err)
	}
}

// positive parses a positive integer query value; empty means def.
func positive(v string, def int) (int, error) {
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		return 0, errors.New("not a positive integer")
	}
	return n, nil
}

// countLines counts the non-empty lines of f and rewinds it.
func countLines(f io.ReadSeeker) (int, error) {
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64<<10), maxLine)
	n := 0
	for sc.Scan() {
		if len(bytes.TrimSpace(sc.Bytes())) > 0 {
			n++
		}
	}
	if err := sc.Err(); err != nil {
		return 0, err
	}
	_, err := f.Seek(0, io.SeekStart)
	return n, err
}

// thinSnapshots copies one snapshot line in every from in to out: lines 0,
// every, 2*every, ... and always the last line, so playback ends on the
// run's final state. A snapshot's "recent" list only holds the events since
// the previous snapshot, so the events of each skipped line are moved into
// the next line that is kept; no event is lost by thinning.
func thinSnapshots(out io.Writer, in io.Reader, every int) error {
	sc := bufio.NewScanner(in)
	sc.Buffer(make([]byte, 0, 64<<10), maxLine)
	var (
		skipped []byte            // the last line not written yet
		carried []json.RawMessage // "recent" of the skipped lines before it
	)
	emit := func(line []byte) error {
		if len(carried) > 0 {
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(line, &fields); err != nil {
				return err
			}
			own, err := recentOf(line)
			if err != nil {
				return err
			}
			if fields["recent"], err = json.Marshal(append(carried, own...)); err != nil {
				return err
			}
			if line, err = json.Marshal(fields); err != nil {
				return err
			}
			carried = nil
		}
		if _, err := out.Write(line); err != nil {
			return err
		}
		_, err := out.Write([]byte{'\n'})
		return err
	}
	// carry moves the pending skipped line's events into carried.
	carry := func() error {
		if skipped == nil {
			return nil
		}
		own, err := recentOf(skipped)
		carried, skipped = append(carried, own...), nil
		return err
	}
	for i := 0; sc.Scan(); {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		if err := carry(); err != nil {
			return err
		}
		if i%every == 0 {
			if err := emit(line); err != nil {
				return err
			}
		} else {
			skipped = append([]byte(nil), line...)
		}
		i++
	}
	if err := sc.Err(); err != nil {
		return err
	}
	if skipped != nil {
		return emit(skipped)
	}
	return nil
}

func recentOf(line []byte) ([]json.RawMessage, error) {
	var s struct {
		Recent []json.RawMessage `json:"recent"`
	}
	err := json.Unmarshal(line, &s)
	return s.Recent, err
}
