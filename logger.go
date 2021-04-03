package usage

import (
	"bytes"
	"io"
	"log"
)

type writer struct {
	out      io.Writer // wrapped output
	filter   [][]byte  // events filtered
	rec      *Recorder // to count filtered events
	category string    // category for filtered events
}

// NewLogger returns a new logger. Events that match a filter string are recorded instead of logging them.
func (r *Recorder) NewLogger(out io.Writer, prefix string, flag int, filter []string, category string) *log.Logger {

	// writer to filter events
	o := &writer{
		out:      out,
		rec:      r,
		category: category,
	}

	// specify filter
	for _, s := range filter {
		o.filter = append(o.filter, []byte(s))
	}

	return log.New(o, prefix, flag)
}

// Write checks for filtered events, and counts them instead of writing them.
// It returns the number of bytes written and any error.
func (w *writer) Write(event []byte) (n int, err error) {

	// look for filtered content
	for _, f := range w.filter {
		if bytes.Contains(event, f) {
			w.rec.Count(string(f), w.category)
			return len(event), nil
		}
	}

	// write unfiltered event
	return w.out.Write(event)
}
