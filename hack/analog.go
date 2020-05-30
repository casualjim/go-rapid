// + build ignore

package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/xtgo/set"

	"github.com/olivere/ndjson"
	"github.com/segmentio/ksuid"
)

type conversationTimeline []conversationEntry

func (c conversationTimeline) Len() int {
	return len(c)
}

func (c conversationTimeline) Less(i, j int) bool {
	if c[i].Time.Before(c[j].Time) {
		return true
	}
	if c[j].Time.Before(c[i].Time) {
		return false
	}
	return strings.Compare(c[i].Instance, c[j].Instance) < 0
}

func (c conversationTimeline) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}

type conversationEntry struct {
	Time     time.Time
	Instance string
	Parents  []ksuid.KSUID
	Data     map[string]interface{}
}

type logEntryConversation struct {
	ID           ksuid.KSUID
	Start        time.Time
	Timeline     conversationTimeline
	Participants []string
	Children     logEntryConversations
}

func (lec *logEntryConversation) Add(le requestLogEntry) {

	pivot := sort.Search(lec.Timeline.Len(), func(i int) bool {
		if le.Time.Equal(lec.Timeline[i].Time) {
			return strings.Compare(lec.Timeline[i].Instance, le.Instance) > 0
		}
		return !lec.Timeline[i].Time.Before(le.Time)
	})

	entry := conversationEntry{
		Time:     le.Time,
		Instance: le.Instance,
		Data:     le.Data,
	}
	if pivot >= len(lec.Timeline) {
		lec.Timeline = append(lec.Timeline, entry)
	} else {
		lec.Timeline = append(lec.Timeline, conversationEntry{})
		copy(lec.Timeline[pivot+1:], lec.Timeline[pivot:])
		lec.Timeline[pivot] = entry
	}
	if le.Time.Before(lec.Start) {
		lec.Start = le.Time
	}
	lec.Participants = set.StringsDo(set.Union, lec.Participants, le.Instance)
}

type rapidSaga struct {
	data         logEntryConversations
	Participants []string
	Times        times
}

type logEntryConversations []logEntryConversation

func (r logEntryConversations) Find(id ksuid.KSUID) (*logEntryConversation, bool) {
	pivot := sort.Search(r.Len(), func(i int) bool {
		if id.IsNil() && !r[i].ID.IsNil() {
			return false
		}
		if !id.IsNil() && r[i].ID.IsNil() {
			return true
		}
		return ksuid.Compare(r[i].ID, id) >= 0
	})
	if pivot < len(r) && r[pivot].ID == id {
		return &r[pivot], true
	}
	return nil, false
}

func (r *logEntryConversations) Add(le requestLogEntry) {
	rr := *r

	if len(le.Parents) > 0 {
		for _, pids := range le.Parents {
			if pids == "" {
				continue
			}
			pid, err := ksuid.Parse(pids)
			if err != nil {
				panic(err)
			}
			if parent, ok := r.Find(pid); ok {
				parent.Children.Add(requestLogEntry{
					ID:       le.ID,
					Time:     le.Time,
					Instance: le.Instance,
					Data:     le.Data,
				})
			}
		}
		return
	}

	pivot := sort.Search(r.Len(), func(i int) bool {
		if le.ID.IsNil() && !rr[i].ID.IsNil() {
			return false
		}
		if !le.ID.IsNil() && rr[i].ID.IsNil() {
			return true
		}
		return ksuid.Compare(rr[i].ID, le.ID) >= 0
	})
	if pivot == len(rr) {
		rr = append(rr, logEntryConversation{
			ID:    le.ID,
			Start: le.Time,
			Timeline: conversationTimeline{
				{Time: le.Time, Instance: le.Instance, Data: le.Data},
			},
			Participants: []string{le.Instance},
		})
	} else if rr[pivot].ID == le.ID {
		rr[pivot].Add(le)
	} else {
		rr = append(rr, logEntryConversation{})
		copy(rr[pivot+1:], rr[pivot:])
		rr[pivot] = logEntryConversation{
			ID:    le.ID,
			Start: le.Time,
			Timeline: conversationTimeline{
				{Time: le.Time, Instance: le.Instance, Data: le.Data},
			},
			Participants: []string{le.Instance},
		}
	}
	*r = rr
}

func (r logEntryConversations) Len() int {
	return len(r)
}

func (r logEntryConversations) Less(i, j int) bool {
	lid, rid := r[i].ID, r[j].ID
	if lid.IsNil() && !rid.IsNil() {
		return false
	}
	if rid.IsNil() && !lid.IsNil() {
		return true
	}
	ids := ksuid.Compare(r[i].ID, r[j].ID)
	if ids != 0 {
		return ids < 0
	}
	return r[i].Start.Before(r[j].Start)
}

func (r logEntryConversations) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}

type times []time.Time

func (t times) Len() int {
	return len(t)
}

func (t times) Less(i, j int) bool {
	return t[i].Before(t[j])
}

func (t times) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

func (r *rapidSaga) Add(data requestLogEntry) {
	if len(data.Parents) > 0 {
		r.data.Add(data)
		return
	}
	r.Participants = set.StringsDo(set.Union, r.Participants, data.Instance)
	ts := append(r.Times, data.Time)
	nt := set.Union(ts, len(r.Times))
	r.Times = ts[:nt]

	r.data.Add(data)
}

func (r *rapidSaga) MaxTime() time.Time {
	return r.Times[len(r.Times)-1]
}

func (r *rapidSaga) MinTime() time.Time {
	return r.Times[0]
}

func (r *rapidSaga) ParticipantRank(name string) int64 {
	return int64(sort.Search(len(r.Participants), func(i int) bool {
		return strings.Compare(r.Participants[i], name) >= 0
	}))
}

func (r *rapidSaga) Sort() {
	set.Uniq(r.Times)
	set.Strings(r.Participants)
	sort.Sort(r.data)
}

type requestLogEntry struct {
	ID       ksuid.KSUID
	Time     time.Time
	Instance string
	Parents  []string
	Data     map[string]interface{}
}

type requestLog []requestLogEntry

// Len is the number of elements in the collection.
func (r requestLog) Len() int {
	return len(r)
}

// Less reports whether the element with
// index i should sort before the element with index j.
func (r requestLog) Less(i int, j int) bool {
	if !r[i].Time.Equal(r[j].Time) {
		return r[i].Time.Before(r[j].Time)
	}
	if strcmp := strings.Compare(r[i].Instance, r[j].Instance); strcmp != 0 {
		return strcmp < 0
	}

	lid, rid := r[i].ID, r[j].ID
	if lid.IsNil() && !rid.IsNil() {
		return false
	}
	if rid.IsNil() && !lid.IsNil() {
		return true
	}
	ids := bytes.Compare(r[i].ID[:], r[j].ID[:])
	return ids < 0
}

func (r requestLog) Filter(predicate func(requestLogEntry) bool) (result requestLog) {
	for _, le := range r {
		if predicate(le) {
			result = append(result, le)
		}
	}
	return
}

// Swap swaps the elements with indexes i and j.
func (r requestLog) Swap(i int, j int) {
	r[i], r[j] = r[j], r[i]
}

type configMismatch struct {
	ID       ksuid.KSUID `json:"request_id"`
	Proposed int64       `json:"proposed"`
	Current  int64       `json:"config"`
}
type configMismatches []configMismatch

func (c configMismatches) Len() int {
	return len(c)
}

func (c configMismatches) Less(i, j int) bool {
	if c[i].Current < c[j].Current {
		return true
	}
	if c[i].Current > c[j].Current {
		return false
	}
	if c[i].Proposed < c[j].Proposed {
		return true
	}
	if c[i].Proposed > c[j].Proposed {
		return false
	}
	return ksuid.Compare(c[i].ID, c[j].ID) < 0
}

func (c configMismatches) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}

func (c configMismatches) Proposed() []int64 {
	proposed := make(Int64Slice, len(c))
	for i, v := range c {
		proposed[i] = v.Proposed
	}
	sort.Sort(proposed)
	pivot := set.Uniq(proposed)
	return proposed[:pivot]
}

func (c configMismatches) Actual() []int64 {
	proposed := make(Int64Slice, len(c))
	for i, v := range c {
		proposed[i] = v.Current
	}
	sort.Sort(proposed)
	pivot := set.Uniq(proposed)
	return proposed[:pivot]
}

func (c configMismatches) Threads() ksuid.CompressedSet {
	result := make([]ksuid.KSUID, len(c))
	var offset int
	for i, v := range c {
		if v.ID.IsNil() {
			offset++
			continue
		}
		result[i-offset] = v.ID
	}
	return ksuid.Compress(result[:len(c)-offset]...)
}

type Int64Slice []int64

func (p Int64Slice) Len() int           { return len(p) }
func (p Int64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p Int64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

type configUpdate struct {
	ID       ksuid.KSUID `json:"request_id"`
	Proposed int64       `json:"proposed"`
	Current  int64       `json:"config"`
}
type configUpdates []configUpdate

func (c configUpdates) Len() int {
	return len(c)
}

func (c configUpdates) Less(i, j int) bool {
	if c[i].Current > c[j].Current {
		return true
	}
	if c[i].Current < c[j].Current {
		return false
	}
	return c[i].Proposed > c[j].Proposed
}

func (c configUpdates) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}

func main() {
	log.SetFlags(0)
	matches, err := filepath.Glob("./internal/membership/*.fail.log")
	if err != nil {
		log.Fatalln(err)
	}

	var rlg, lgChanged, lgSafe, lgUpdated requestLog
	var mismatches configMismatches
	var updates configUpdates

	//var triggers ksuid.Sequence
	for _, m := range matches {
		if strings.HasSuffix(m, "actual.log") || strings.HasSuffix(m, "act.log") {
			continue
		}

		lgf, err := os.Open(m)
		if err != nil {
			log.Fatalln(err)
		}

		dec := ndjson.NewReader(lgf)
		for dec.Next() {
			var data map[string]interface{}
			if err := dec.Decode(&data); err != nil {
				_ = lgf.Close()
				log.Fatalln(err)
			}

			le := requestLogEntry{
				Data:     data,
				ID:       mustParseKsuid(data["request_id"]),
				Time:     mustParseTime(data["time"]),
				Parents:  parseStringSlice(data, "triggers"),
				Instance: data["instance"].(string),
			}
			rlg = append(rlg, le)
			if rp, ok := le.Data["resp"].(string); ok {
				switch rp {
				case "CONFIG_CHANGED":
					lgChanged = append(lgChanged, le)
				case "SAFE_TO_JOIN":
					lgSafe = append(lgSafe, le)
				}
			}
			if rp, ok := le.Data["message"].(string); ok {
				switch rp {
				case "config updated":
					lgUpdated = append(lgUpdated, le)
				case "config update":
					updates = append(updates, configUpdate{
						ID:       le.ID,
						Proposed: mustParseInt64(le.Data, "new_config"),
						Current:  mustParseInt64(le.Data, "previous_config"),
					})
				case "configuration mismatch":
					var el configMismatch
					el.ID = le.ID
					el.Proposed = mustParseInt64(le.Data, "proposed")
					el.Current = mustParseInt64(le.Data, "config")
					mismatches = append(mismatches, el)
				}

			}
		}
		if dec.Err() != nil {
			_ = lgf.Close()
			log.Fatalln(dec.Err())
		}
		if err = lgf.Close(); err != nil {
			log.Fatalln(err)
		}
	}

	sort.Sort(rlg)
	sort.Sort(lgChanged)
	sort.Sort(lgSafe)
	sort.Sort(lgUpdated)
	sort.Sort(mismatches)
	sort.Sort(updates)

	saga := new(rapidSaga)
	sagaChanged := new(rapidSaga)
	sagaSafe := new(rapidSaga)
	for _, le := range rlg {
		if le.ID.IsNil() {
			continue
		}
		saga.Add(le)
	}
	for _, le := range lgChanged {
		if le.ID.IsNil() {
			continue
		}
		sagaChanged.Add(le)
	}

	// changed data gives us the end requests that failed.
	// The failed request is used to look up the message "config update"

	log.Println("participants:", saga.Participants)
	log.Println("started at:", saga.MinTime().Format(time.StampNano))
	log.Println("ended at:", saga.MaxTime().Format(time.StampNano))
	log.Printf("no of conversations: total=%d changed=%d safe=%d", len(saga.data), len(sagaChanged.data), len(sagaSafe.data))
	log.Println("no of entries", len(saga.Times))
	log.Println("no of participants", len(saga.Participants))

	for _, le := range saga.data {
		if len(le.Children) > 0 {
			ss := make([]string, len(le.Children))
			for i, par := range le.Children {
				ss[i] = par.ID.String()
			}
			log.Printf("parent: %s -> [%s]", le.ID.String(), strings.Join(ss, ", "))
			for _, l := range le.Timeline {
				log.Println("   ", l.Time.Format(time.Stamp)+"|"+l.Instance+":", l.Data["message"])
			}
			for _, p := range le.Children {
				for _, l := range p.Timeline {

					log.Println("      ", l.Time.Format(time.Stamp)+"|"+l.Instance+":", l.Data["message"])
					//log.Println("      ", l.Time.Format(time.Stamp)+"|"+l.Instance+":", pretty.Sprint(l.Data))
				}
			}
		}
	}

	for _, le := range sagaChanged.data {
		convo, ok := saga.data.Find(le.ID)
		if !ok {
			log.Fatalln("couldn't find conversation in saga database")
		}
		log.Printf("changed: id=%s participants=%v parents=%v", convo.ID, convo.Participants, len(convo.Children))
		for _, l := range rlg.Filter(func(lle requestLogEntry) bool { return lle.ID == le.ID }) {
			log.Println("   ", l.Time.Format(time.Stamp)+"|"+l.Instance+":", l.Data["message"])
		}
	}
	log.Println("config mismatches")
	log.Printf("  current=%v proposed=%v threads=%v", mismatches.Actual(), mismatches.Proposed(), mismatches.Threads())

	log.Println("config updates")
	for _, l := range updates {
		log.Printf("  previous=%v new=%v threads=%s", l.Current, l.Proposed, l.ID)
	}
	//for _, e := range saga.data {
	//	log.Println("  conversation:", e.ID.String(), e.Participants)
	//	for _, l := range e.Timeline {
	//		log.Println("   ", l.Time.Format(time.Stamp)+"|"+l.Instance+":", l.Data["message"])
	//	}
	//}

	//writeAsCSV(rlg, saga)

	type chartEntry struct {
		ID        ksuid.KSUID
		Instances []string
	}
	var tms []int64
	entries := make(map[int64][]chartEntry)
	prevInstanceForThread := make(map[ksuid.KSUID]string)

	for _, v := range rlg {
		if prev, known := prevInstanceForThread[v.ID]; known && prev == v.Instance {
			continue
		}

		prevInstanceForThread[v.ID] = v.Instance
		tms = append(tms, v.Time.UnixNano())
		if ev, ok := entries[v.Time.UnixNano()]; ok {
			var found bool
			for i := range ev {
				ff := &ev[i]
				if ff.ID == v.ID {
					ff.Instances = set.StringsDo(set.Union, ff.Instances, v.Instance)
					found = true
					break
				}
			}
			if !found {
				entries[v.Time.UnixNano()] = append(ev, chartEntry{
					ID:        v.ID,
					Instances: []string{v.Instance},
				})
			} else {
				entries[v.Time.UnixNano()] = ev
			}

		} else {
			entries[v.Time.UnixNano()] = append(entries[v.Time.UnixNano()], chartEntry{
				ID:        v.ID,
				Instances: []string{v.Instance},
			})
		}
	}
	type renderItem struct {
		Time     int64
		Instance string
	}
	series := make(map[ksuid.KSUID][]renderItem)
	for _, tm := range tms {
		if dd, ok := entries[tm]; ok {
			for _, e := range dd {
				for _, inst := range e.Instances {
					series[e.ID] = append(series[e.ID], renderItem{
						Time:     tm,
						Instance: inst,
					})
				}

			}
		} else {
			panic("missing time")
		}
	}

	jsonf, _ := os.Create("analysis.json")
	json.NewEncoder(jsonf).Encode(struct {
		AllTimes      []int64                      `json:"allTimes"`
		Participants  []string                     `json:"participants"`
		Conversations map[ksuid.KSUID][]renderItem `json:"conversations"`
	}{
		AllTimes:      tms,
		Participants:  saga.Participants,
		Conversations: series,
	})
	jsonf.Close()
	//
	//if serr := http.ListenAndServe(":4921", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	//	line := charts.NewLine()
	//	line.SetGlobalOptions(charts.TitleOpts{
	//		Title: "Rapid Check Race",
	//	}, charts.XAxisOpts{Data: saga.Participants})
	//	line.AddXAxis(tms)
	//	for k, v := range series {
	//		line.AddYAxis(k.String(), v)
	//	}
	//	line.Render(w)
	//})); serr != nil && !errors.Is(err, http.ErrServerClosed) {
	//	log.Fatalln(err)
	//}

}

func writeAsCSV(rlg requestLog, saga *rapidSaga) {
	w := csv.NewWriter(os.Stdout)
	for _, le := range rlg {
		w.Write([]string{
			le.Time.Format(time.RFC3339Nano),
			strconv.FormatInt(le.Time.UnixNano(), 10),
			le.Instance,
			strconv.FormatInt(saga.ParticipantRank(le.Instance), 10),
			le.ID.String(),
			strings.Join(le.Parents, "|"),
			le.Data["message"].(string),
		})
	}
	w.Flush()
	if w.Error() != nil {
		log.Fatalln(w.Error())
	}
}

func parseStringSlice(data map[string]interface{}, key string) []string {
	if v, has := data[key]; has {
		if s, isSlice := v.([]interface{}); isSlice {
			var ss []string
			for _, vl := range s {
				if sv, isStr := vl.(string); isStr && sv != "" {
					ss = append(ss, sv)
				}
			}
			return ss
		}
	}
	return nil
}

func mustParseInt64(data map[string]interface{}, key string) int64 {
	if v, ok := data[key].(float64); ok {
		return int64(v)
	}
	return 0
}

func mustParseTime(val interface{}) time.Time {
	vs := val.(string)
	if vs == "" {
		panic("time can't be empty")
	}
	tm, err := time.Parse(time.RFC3339Nano, vs)
	if err != nil {
		panic(err)
	}
	return tm
}

func mustParseKsuid(val interface{}) ksuid.KSUID {
	if val == nil {
		return ksuid.Nil
	}
	vs := val.(string)
	if vs == "" {
		panic("request_id can't be empty")
	}
	ks, err := ksuid.Parse(vs)
	if err != nil {
		panic(err)
	}
	return ks
}
