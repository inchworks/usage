// Copyright © Rob Burke inchworks.com, 2020.

// inchworks/usage is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// inchworks/usage is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with inchworks/usage.  If not, see <https://www.gnu.org/licenses/>.

// Package usage generates statistics for an internet service, such as a web server.
// User identities and IP addresses can be anonymised at the time of collection, so that no personal data is stored.
// Therefore there should be no need under the EU GDPR to request permission for data collection.
package usage

// Note:
//  o Time periods are UTC.

import (
	"math/rand"
	"net"
	"sort"
	"strconv"
	"sync"
	"time"
)

const (
	tickDetail = time.Minute      // determines accuracy of periods
	saveDetail = time.Minute * 10 // determines data loss on server crash
)

// Level of detail for statistic
type Detail int

const (
	Seen  Detail = 0
	Base  Detail = 1
	Day   Detail = 2
	Month Detail = 3
	Year  Detail = 4 // reserved, not used
	Mark  Detail = 5
)

// Anonymisation level
type Anonymise int

const (
	Daily Anonymise = iota
	Immediate
)

// Statistic struct holds the stored data for a usage statistic.
type Statistic struct {
	Id       int64
	Event    string
	Category string
	Count    int
	Detail   Detail
	Start    time.Time
}

// StatisticsStore is the interface implemented by the parent application for storage and update of usage statistics.
//
// The unique key for a statistic is: Event, Start, Detail.
type StatisticStore interface {

	// BeforeByCategory returns statistics ordered by Category and Start.
	BeforeByCategory(before time.Time, detail Detail) []*Statistic

	// BeforeByCategory returns statistics ordered by Event and Start.
	BeforeByEvent(before time.Time, detail Detail) []*Statistic

	// BeforeByTime returns statistics for a level of detail, ordered by Start DESC and then preferred display order (e.g. Count DESC).
	BeforeByTime(before time.Time, detail Detail) []*Statistic

	// DeleteId deletes a statistic.
	DeleteId(id int64) error

	// DeleteIf deletes earlier statistics for the period.
	DeleteOld(before time.Time, detail Detail) error

	// GetEvent gets the statistic for an event.
	GetEvent(event string, start time.Time, detail Detail) *Statistic

	// GetMark gets the named point in time.
	GetMark(event string) *Statistic

	// Transaction starts a database transaction. It returns a function to be called to end the transaction.
	Transaction() func()

	// Update adds or updates a statistic.
	Update(s *Statistic) error
}

// Recorder holds the state for recording stats.
type Recorder struct {
	store StatisticStore

	// parameters
	anon         Anonymise
	base         Detail        // Base or Day
	baseDuration time.Duration // e.g. an hour
	keepBase     int           // in days
	keepDays     int           // in days
	keepEvents   int           // in months
	keepMonths   int           // in months

	chSaver *time.Ticker
	chDone  chan bool

	// operation times
	periodStart time.Time
	now         time.Time // (set on each tick, for constency and to allow accelerated testing)
	volatileEnd time.Time
	periodEnd   time.Time
	dayEnd      time.Time

	// volatile stats
	mu       sync.Mutex
	count    map[string]int
	seen     map[string]bool
	category map[string]string

	// anonymised IDs
	anonymised map[int64]string
}

// New returns a Recorder, ready to record and aggregate events.
func New(st StatisticStore, anon Anonymise, baseDur time.Duration, baseDays int, days int, eventMonths int, months int) (*Recorder, error) {

	// override silly parameters with defaults
	if baseDur < time.Hour || baseDur > time.Hour*24 {
		baseDur = time.Hour * 24
	}
	if baseDays < 1 {
		baseDays = 1
	}
	if days < 1 {
		days = 7
	}
	if eventMonths < 1 {
		eventMonths = 3
	}
	if months < 1 {
		months = 24
	}

	r := &Recorder{
		anon:         anon,
		store:        st,
		baseDuration: baseDur,
		keepBase:     baseDays,
		keepDays:     days,
		keepEvents:   eventMonths,
		keepMonths:   months,

		now: time.Now().UTC(),

		chSaver: time.NewTicker(tickDetail),
		chDone:  make(chan bool, 1),

		count:    make(map[string]int),
		seen:     make(map[string]bool),
		category: make(map[string]string),

		anonymised: make(map[int64]string),
	}

	if r.baseDuration != time.Hour*24 {
		r.base = Base
	} else {
		r.base = Day
	}

	// start of statistics recording
	if err := r.markLive(); err != nil {
		return nil, err
	}

	// next operations
	r.periodStart = r.start(baseDur)
	r.volatileEnd = r.next(saveDetail)
	r.periodEnd = r.next(baseDur)
	r.dayEnd = r.start(time.Hour * 24)

	// start saver
	go r.saver(r.chSaver.C, r.chDone)

	return r, nil
}

// Count increments the count for an event.
func (r *Recorder) Count(event string, category string) {

	if event == "" {
		event = "#"
	} // something searchable for home page

	r.mu.Lock()
	defer r.mu.Unlock()

	r.count[event]++             // count events
	r.category[event] = category // ## note aggregate - inefficient
}

// FormatID returns an anonymised name for a database ID, such as the ID of a logged-on user.
// It is intended to be used as an event name for usage.Seen.
func (r *Recorder) FormatID(prefix string, id int64) string {

	var s string
	switch r.anon {

	case Daily:
		// use the real ID, records are discarded daily
		s = strconv.FormatUint(uint64(id), 36)

	case Immediate:
		fallthrough

	default:
		// use randomised IDs
		var seen bool
		s, seen = r.anonymised[id]
		if !seen {
			// remember new randomised ID
			s = strconv.FormatUint(uint64(rand.Uint32()), 36)
			r.anonymised[id] = s
		}
	}

	return prefix + s
}

// FormatIP returns an anonymised IP address.
// It is intended to be used as an event name for usage.Seen.
func FormatIP(addr string) string {

	// This is more complex than I hoped, because the format of the IP address is "IP:port"
	// and we might not have a valid address.

	ipStr, _, err := net.SplitHostPort(addr)
	if err != nil {
		return ""
	}

	// convert IP address to slice of 16 bytes
	ip := net.ParseIP(ipStr)
	if ip == nil {
		return ""
	}

	// attempt to reduce it to 4 bytes
	ip4 := ip.To4()

	// anonymise address
	// (ICANN method 4, https://www.icann.org/en/system/files/files/rssac-040-07aug18-en.pdf)
	if ip4 != nil {
		ip4[3] = 0
		ipStr = ip4.String()

	} else if len(ip) == 16 {
		for i := 6; i < 16; i++ {
			ip[i] = 0
		}
		ipStr = ip.String()

	} else {
		return "" // unknown - cannot anonymise
	}

	return ipStr
}

// Get returns all statistics for base periods, days or months.
// Statistics for a single period are grouped into a separate slice.
func Get(st StatisticStore, ival Detail) [][]*Statistic {

	sPeriods := make([][]*Statistic, 0, 16)
	var stats []*Statistic
	var before time.Time

	// rollup lower level stats, and split into start periods
	switch ival {
	case Month:
		// days into months
		// ## we're missing the base periods
		statsDays := st.BeforeByEvent(time.Now().UTC(), ival-1)
		stats, before = getRollup(st, statsDays, forMonth, Month)
		sPeriods = split(sPeriods, stats)

	case Day:
		// base periods into days
		statsBase := st.BeforeByEvent(time.Now().UTC(), ival-1)
		stats, before = getRollup(st, statsBase, forDay, Day)
		sPeriods = split(sPeriods, stats)

	default:
		before = time.Now().UTC()
	}

	// add in the remaining periods, for which rollup wasn't needed
	stats = st.BeforeByTime(before, ival)
	sPeriods = split(sPeriods, stats)

	// replace seen counts by daily average
	if ival == Month {
		average(st, sPeriods)
	}

	return sPeriods
}

// Mark records a point in time, such as the start of recording statistics.
func (r *Recorder) Mark(event string, category string) error {

	s := &Statistic{
		Event:    event,
		Category: category,
		Start:    time.Now().UTC(),
		Detail:   Mark,
	}

	defer r.store.Transaction()()
	return r.store.Update(s)
}

// Seen counts distinct events seen within a period (such as visitors to a website).
func (r *Recorder) Seen(event string, category string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.seen[event] = true         // event occurred
	r.category[event] = category // ## note aggregate - inefficient
}

// Stop terminates recording.
func (r *Recorder) Stop() {

	r.chSaver.Stop()
	r.chDone <- true
}

// aggregate combines detail or seen events into a parent category.
func (r *Recorder) aggregate(stats []*Statistic, newCategory string, ival Detail) {

	st := r.store

	var category string
	var start time.Time
	var newEvent string
	var next bool
	var total *Statistic

	for _, s := range stats {

		// exclude events that have already been aggregated
		// i.e. distinct events, already aggregated, and lower level aggregates
		if s.Event[:1] != "<" {

			count := s.Count
			if count == 0 {
				count = 1 // not set for seen events
			}

			// aggregate events for same category and time
			if category != s.Category {
				category = s.Category
				newEvent = "<" + s.Category + ">"
				next = true
			}

			if start != s.Start {
				start = s.Start
				next = true
			}

			if next {
				// save previous total
				if total != nil {
					st.Update(total)
				}

				// next total
				total = st.GetEvent(newEvent, start, Day)
				if total == nil {
					// new total
					total = &Statistic{
						Event:    newEvent,
						Category: newCategory,
						Start:    start,
						Detail:   ival,
					}
				}
				next = false
			}

			// add event to total and drop event
			total.Count += count
			st.DeleteId(s.Id)
		}
	}
	if total != nil {
		st.Update(total) // save final total
	}
}

// aggregateEvents combines events into categories
func (r *Recorder) aggregateEvents(before time.Time, ival Detail) {

	stats := r.store.BeforeByCategory(before, ival) // ordered by category and time

	r.aggregate(stats, "total", ival)
}

// Count discrete events seen for day
// Must include previous days, in case server wasn't running at day end.

func (r *Recorder) aggregateSeen() {

	stats := r.store.BeforeByCategory(r.dayEnd, Seen) // ordered by category and time

	r.aggregate(stats, "seen", Day)

}

// average converts seen counts to daily averages.
//
// Because because we can't distinguish between visitors across days.
func average(st StatisticStore, sPeriods [][]*Statistic) {

	// ## Is there an event we could distinguish and should support? E.g. errors?

	// first month of recording
	s := st.GetMark("goLive")
	if s == nil {
		return // conversion not possible
	}
	yLive := s.Start.Year()
	mLive := s.Start.Month()
	dLive := s.Start.Day()

	// current month of recording
	now := time.Now().UTC()
	yNow := now.Year()
	mNow := now.Month()
	dNow := now.Day()

	for _, ss := range sPeriods {

		// calculate no of days in period
		var days int
		y := ss[0].Start.Year()
		m := ss[0].Start.Month()

		if m == mNow && y == yNow {
			// current month
			days = dNow
		} else {
			// days in month
			days = daysIn(y, m)
		}

		if m == mLive && y == yLive {
			// first month - this applies even when the first month is also the current month
			days = days - dLive + 1
		}

		for _, s := range ss {
			// convert seen counts to daily average
			if s.Category == "seen" {
				s.Category = "daily"
				s.Count = int(s.Count+(days+1)/2-1) / days // rounded nearest
			}
		}
	}
}

// catchUp is called on startup to do any missed processing while the service was shut down.
func (r *Recorder) catchUp() {

	r.doDaily()
}

// daysIn returns the number of days in a month.
func daysIn(year int, month time.Month) int {

	// works because values outside the normal range are normalised
	return time.Date(year, month+1, 0, 0, 0, 0, 0, time.UTC).Day()
}

// doDaily does processing at the end of a day.
func (r *Recorder) doDaily() {

	// make changes as a transaction
	st := r.store
	defer st.Transaction()()

	// count discrete events seen during day
	r.aggregateSeen()

	// rollup base period into days - no-op if base is a day
	if r.base == Base {
		r.rollupPeriod(r.dayEnd.AddDate(0, 0, -r.keepBase), Base, forDay, Day)
	}

	// ## could aggregate days, if wanted

	// rollup days into months
	r.rollupPeriod(r.dayEnd.AddDate(0, 0, -r.keepDays), Day, forMonth, Month)

	// aggregate events into categories
	r.aggregateEvents(r.dayEnd.AddDate(0, -r.keepEvents, 0), Month)

	// purge months
	st.DeleteOld(r.dayEnd.AddDate(0, -r.keepMonths, 0), Month)

	// next day
	r.dayEnd = r.dayEnd.AddDate(0, 0, 1)
}

// doPeriodically does processing at the end of a base period, such as hourly.
func (r *Recorder) doPeriodically() {

	// nothing to do, except start saving for next period
	r.periodStart = r.start(r.baseDuration)
	r.periodEnd = r.next(r.baseDuration)
}

// forDay returns the start time of a day
func forDay(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}

// forMonth returns the start time of a month
func forMonth(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC)
}

// getRollup combines recent events into the parent period for display.
// It returns a slice of stats, sorted by time and count, and oldest start included.
func getRollup(st StatisticStore, stats []*Statistic, toStart func(time.Time) time.Time, to Detail) ([]*Statistic, time.Time) {

	// ## similar pattern to aggregate() and rollup() - could it be combined?

	ss := make([]*Statistic, 0, 32)

	var event string
	var start time.Time
	var next bool
	var total *Statistic

	for _, s := range stats {

		// next parent period?
		sStart := toStart(s.Start)
		if start != sStart {
			start = sStart
			next = true
		}

		// next event?
		if event != s.Event {
			event = s.Event
			next = true
		}

		if next {

			if total != nil {

				// add in parent period
				// ## not efficient - read and index them?
				if p := st.GetEvent(total.Event, total.Start, to); p != nil {
					total.Count += p.Count
				}

				// save previous total
				ss = append(ss, total)
			}

			// next total
			total = &Statistic{
				Event:    event,
				Category: s.Category,
				Start:    start,
				Detail:   to,
			}
			next = false
		}

		// add event to total
		total.Count += s.Count
	}
	if total != nil {

		// add in parent period
		if p := st.GetEvent(total.Event, total.Start, to); p != nil {
			total.Count += p.Count
		}

		ss = append(ss, total) // save final total
	}

	// sort by time descending, count descending, event
	compare := func(i, j int) bool {
		if ss[i].Start == ss[j].Start {
			if ss[i].Count == ss[j].Count {
				return ss[i].Event < ss[j].Event
			} else {
				return ss[i].Count > ss[j].Count
			}
		} else {
			return ss[i].Start.After(ss[j].Start)
		}
	}
	sort.Slice(ss, compare)

	// processed period
	if len(ss) > 0 {
		start = ss[len(ss)-1].Start
	} else {
		start = time.Now().UTC()
	}

	return ss, start
}

// markLive marks the start of live operation
func (r *Recorder) markLive() error {

	st := r.store
	s := st.GetMark("goLive")
	if s == nil {

		// server is starting for the first time
		s = &Statistic{
			Event:    "goLive",
			Category: "timeline",
			Start:    r.now,
			Detail:   Mark,
		}

		defer st.Transaction()()
		if err := st.Update(s); err != nil {
			return err
		}
	}

	return nil
}

// next returns the time of the next operation, UTC, aligned to detail
func (r *Recorder) next(detail time.Duration) time.Time {
	return r.now.Truncate(detail).Add(detail)
}

// rollup combines events into the parent period
func (r *Recorder) rollup(stats []*Statistic, toStart func(time.Time) time.Time, to Detail) {

	// ## similar pattern to aggregate() - could it be combined?

	st := r.store

	var event string
	var start time.Time
	var next bool
	var total *Statistic

	for _, s := range stats {

		// aggregate events for parent period
		if event != s.Event {
			event = s.Event
			next = true
		}

		sStart := toStart(s.Start)
		if start != sStart {
			start = sStart
			next = true
		}

		if next {
			// save previous total
			if total != nil {
				st.Update(total)
			}

			// next total
			total = st.GetEvent(event, start, to)
			if total == nil {
				// new total
				total = &Statistic{
					Event:    event,
					Category: s.Category,
					Start:    start,
					Detail:   to,
				}
			}
			next = false
		}

		// add event to total and drop event
		total.Count += s.Count
		st.DeleteId(s.Id)
	}
	if total != nil {
		st.Update(total) // save final total
	}
}

// rollupPeriod combines statistics into the next level.
func (r *Recorder) rollupPeriod(before time.Time, from Detail, toStart func(time.Time) time.Time, to Detail) {

	stats := r.store.BeforeByEvent(before, from)

	r.rollup(stats, toStart, to)
}

// save sets event counts in the database. It is called at least once per hour, and on shutdown.
func (r *Recorder) save() {

	// copy the volatile counts and unlock them, so we don't block the user unnecessarily
	r.mu.Lock()

	count := make([]struct {
		evt string
		n   int
	}, len(r.count))
	i := 0
	for event, n := range r.count {
		count[i].evt = event
		count[i].n = n
		i++
	}

	seen := make([]string, len(r.seen))
	i = 0
	for evt := range r.seen {
		seen[i] = evt
		i++
	}

	category := make(map[string]string, len(r.category))
	for evt, grp := range r.category {
		category[evt] = grp
	}

	// reset the volatile counts (deletions optimised by compiler)
	for k := range r.count {
		delete(r.count, k)
	}
	for k := range r.seen {
		delete(r.seen, k)
	}
	for k := range r.category {
		delete(r.category, k)
	}

	// delete mappings daily, so that visit counts are reset
	if r.now.After(r.dayEnd) {
		r.anonymised = make(map[int64]string)
	}

	r.mu.Unlock()

	// make database changes as a transaction
	st := r.store
	defer st.Transaction()()

	// save volatile counts
	for _, ec := range count {

		s := st.GetEvent(ec.evt, r.periodStart, r.base)
		if s == nil {
			// new statistic
			s = &Statistic{
				Event:    ec.evt,
				Category: category[ec.evt],
				Count:    ec.n,
				Start:    r.periodStart,
				Detail:   r.base,
			}
		} else {
			// update statistic
			s.Count += ec.n
		}
		st.Update(s)
	}

	// save distinct events
	for _, evt := range seen {

		s := st.GetEvent(evt, r.periodStart, Seen)
		if s == nil {
			// new event seen
			s = &Statistic{
				Event:    evt,
				Category: category[evt],
				Count:    1,
				Start:    r.periodStart,
				Detail:   Seen,
			}
			st.Update(s)
		}
	}

	// next save
	r.volatileEnd = r.next(saveDetail)
}

// saver is a goroutine to make asynchronous aggregates and saves to the database.
func (r *Recorder) saver(chTick <-chan time.Time, chDone <-chan bool) {

	// do anything pending during shutdown
	// (also helps with testing)
	r.now = time.Now().UTC()
	r.catchUp()

	for {
		select {
		case t := <-chTick:
			r.now = t.UTC()

			if r.now.After(r.volatileEnd) {
				r.save()
			}
			if r.now.After(r.periodEnd) {
				r.doPeriodically()
			}
			if r.now.After(r.dayEnd) {
				r.doDaily()
			}

		case <-chDone:
			r.now = time.Now().UTC()
			r.save() // save volatile counts before shutdown
			return
		}
	}
}

// split groups stats by start time. the Input slice assumed to be sorted.
func split(sPeriods [][]*Statistic, stats []*Statistic) [][]*Statistic {

	var last time.Time
	var ss []*Statistic

	// split stats into periods
	for _, s := range stats {
		if s.Start != last {
			if len(ss) > 0 {
				sPeriods = append(sPeriods, ss)
			}
			ss = make([]*Statistic, 0, 32)
			last = s.Start
		}
		ss = append(ss, s)
	}

	// final period
	if len(ss) > 0 {
		sPeriods = append(sPeriods, ss)
	}

	return sPeriods
}

// start returns the start of a period, UTC, aligned to detail.
func (r *Recorder) start(detail time.Duration) time.Time {
	return r.now.Truncate(detail)
}
