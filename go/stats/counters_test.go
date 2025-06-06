/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package stats

import (
	"expvar"
	"math/rand/v2"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCounters(t *testing.T) {
	clearStats()
	c := NewCountersWithSingleLabel("counter1", "help", "label")
	c.Add("c1", 1)
	c.Add("c2", 1)
	c.Add("c2", 1)
	want1 := `{"c1": 1, "c2": 2}`
	want2 := `{"c2": 2, "c1": 1}`
	if s := c.String(); s != want1 && s != want2 {
		t.Errorf("want %s or %s, got %s", want1, want2, s)
	}
	counts := c.Counts()
	if counts["c1"] != 1 {
		t.Errorf("want 1, got %d", counts["c1"])
	}
	if counts["c2"] != 2 {
		t.Errorf("want 2, got %d", counts["c2"])
	}
}

func TestCountersTags(t *testing.T) {
	clearStats()
	c := NewCountersWithSingleLabel("counterTag1", "help", "label")
	want := map[string]int64{}
	got := c.Counts()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("want %v, got %v", want, got)
	}

	c = NewCountersWithSingleLabel("counterTag2", "help", "label", "tag1", "tag2")
	want = map[string]int64{"tag1": 0, "tag2": 0}
	got = c.Counts()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("want %v, got %v", want, got)
	}
}

func TestMultiCounters(t *testing.T) {
	clearStats()
	c := NewCountersWithMultiLabels("mapCounter1", "help", []string{"aaa", "bbb"})
	c.Add([]string{"c1a", "c1b"}, 1)
	c.Add([]string{"c2a", "c2b"}, 1)
	c.Add([]string{"c2a", "c2b"}, 1)
	want1 := `{"c1a.c1b": 1, "c2a.c2b": 2}`
	want2 := `{"c2a.c2b": 2, "c1a.c1b": 1}`
	if s := c.String(); s != want1 && s != want2 {
		t.Errorf("want %s or %s, got %s", want1, want2, s)
	}
	counts := c.Counts()
	if counts["c1a.c1b"] != 1 {
		t.Errorf("want 1, got %d", counts["c1a.c1b"])
	}
	if counts["c2a.c2b"] != 2 {
		t.Errorf("want 2, got %d", counts["c2a.c2b"])
	}
	f := NewCountersFuncWithMultiLabels("", "help", []string{"aaa", "bbb"}, func() map[string]int64 {
		return map[string]int64{
			"c1a.c1b": 1,
			"c2a.c2b": 2,
		}
	})
	if s := f.String(); s != want1 && s != want2 {
		t.Errorf("want %s or %s, got %s", want1, want2, s)
	}
}

func TestMultiCountersDot(t *testing.T) {
	clearStats()
	c := NewCountersWithMultiLabels("mapCounter2", "help", []string{"aaa", "bbb"})
	c.Add([]string{"c1.a", "c1b"}, 1)
	c.Add([]string{"c2a", "c2.b"}, 1)
	c.Add([]string{"c2a", "c2.b"}, 1)
	c1a := safeLabel("c1.a")
	c1aJSON := strings.ReplaceAll(c1a, "\\", "\\\\")
	c2b := safeLabel("c2.b")
	c2bJSON := strings.ReplaceAll(c2b, "\\", "\\\\")
	want1 := `{"` + c1aJSON + `.c1b": 1, "c2a.` + c2bJSON + `": 2}`
	want2 := `{"c2a.` + c2bJSON + `": 2, "` + c1aJSON + `.c1b": 1}`
	if s := c.String(); s != want1 && s != want2 {
		t.Errorf("want %s or %s, got %s", want1, want2, s)
	}
	counts := c.Counts()
	if counts[c1a+".c1b"] != 1 {
		t.Errorf("want 1, got %d", counts[c1a+".c1b"])
	}
	if counts["c2a."+c2b] != 2 {
		t.Errorf("want 2, got %d", counts["c2a."+c2b])
	}
}

func TestCountersHook(t *testing.T) {
	var gotname string
	var gotv *CountersWithSingleLabel
	clearStats()
	Register(func(name string, v expvar.Var) {
		gotname = name
		gotv = v.(*CountersWithSingleLabel)
	})

	v := NewCountersWithSingleLabel("counter2", "help", "label")
	if gotname != "counter2" {
		t.Errorf("want counter2, got %s", gotname)
	}
	if gotv != v {
		t.Errorf("want %#v, got %#v", v, gotv)
	}
}

var benchCounter = NewCountersWithSingleLabel("bench", "help", "label")

func BenchmarkCounters(b *testing.B) {
	clearStats()
	benchCounter.Add("c1", 1)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			benchCounter.Add("c1", 1)
		}
	})
}

var benchMultiCounter = NewCountersWithMultiLabels("benchMulti", "help", []string{"call", "keyspace", "dbtype"})

func BenchmarkMultiCounters(b *testing.B) {
	clearStats()
	key := []string{"execute-key-ranges", "keyspacename", "replica"}
	benchMultiCounter.Add(key, 1)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			benchMultiCounter.Add(key, 1)
		}
	})
}

func BenchmarkCountersTailLatency(b *testing.B) {
	// For this one, ignore the time reported by 'go test'.
	// The 99th Percentile log line is all that matters.
	// (Cmd: go test -bench=BenchmarkCountersTailLatency -benchtime=30s -cpu=10)
	clearStats()
	benchCounter.Add("c1", 1)
	c := make(chan time.Duration, 100)
	done := make(chan struct{})
	go func() {
		all := make([]int, b.N)
		i := 0
		for dur := range c {
			all[i] = int(dur)
			i++
		}
		sort.Ints(all)
		p99 := time.Duration(all[b.N*99/100])
		b.Logf("99th Percentile (for N=%v): %v", b.N, p99)
		close(done)
	}()

	b.ResetTimer()
	b.SetParallelism(100) // The actual number of goroutines is 100*GOMAXPROCS
	b.RunParallel(func(pb *testing.PB) {
		var start time.Time

		for pb.Next() {
			// sleep between 0~200ms to simulate 10 QPS per goroutine.
			time.Sleep(time.Duration(rand.Int64N(200)) * time.Millisecond)
			start = time.Now()
			benchCounter.Add("c1", 1)
			c <- time.Since(start)
		}
	})
	b.StopTimer()

	close(c)
	<-done
}

func TestCountersFuncWithMultiLabels(t *testing.T) {
	clearStats()
	f := NewCountersFuncWithMultiLabels("TestCountersFuncWithMultiLabels", "help", []string{"label1"}, func() map[string]int64 {
		return map[string]int64{
			"c1": 1,
			"c2": 2,
		}
	})

	want1 := `{"c1": 1, "c2": 2}`
	want2 := `{"c2": 2, "c1": 1}`
	if s := f.String(); s != want1 && s != want2 {
		t.Errorf("want %s or %s, got %s", want1, want2, s)
	}
}

func TestCountersFuncWithMultiLabels_Hook(t *testing.T) {
	var gotname string
	var gotv *CountersFuncWithMultiLabels
	clearStats()
	Register(func(name string, v expvar.Var) {
		gotname = name
		gotv = v.(*CountersFuncWithMultiLabels)
	})

	v := NewCountersFuncWithMultiLabels("TestCountersFuncWithMultiLabels_Hook", "help", []string{"label1"}, func() map[string]int64 {
		return map[string]int64{}
	})
	if gotname != "TestCountersFuncWithMultiLabels_Hook" {
		t.Errorf("want TestCountersFuncWithMultiLabels_Hook, got %s", gotname)
	}
	if gotv != v {
		t.Errorf("want %#v, got %#v", v, gotv)
	}
}

func TestCountersCombineDimension(t *testing.T) {
	clearStats()
	// Empty labels shouldn't be combined.
	c0 := NewCountersWithSingleLabel("counter_combine_dim0", "help", "")
	c0.Add("c1", 1)
	assert.Equal(t, `{"c1": 1}`, c0.String())

	clearStats()
	combineDimensions = "a,c"

	c1 := NewCountersWithSingleLabel("counter_combine_dim1", "help", "label")
	c1.Add("c1", 1)
	assert.Equal(t, `{"c1": 1}`, c1.String())

	c2 := NewCountersWithSingleLabel("counter_combine_dim2", "help", "a")
	c2.Add("c1", 1)
	assert.Equal(t, `{"all": 1}`, c2.String())

	c3 := NewCountersWithSingleLabel("counter_combine_dim3", "help", "a")
	assert.Equal(t, `{"all": 0}`, c3.String())

	// Anything under "a" and "c" should get reported under a consolidated "all" value
	// instead of the specific supplied values.
	c4 := NewCountersWithMultiLabels("counter_combine_dim4", "help", []string{"a", "b", "c"})
	c4.Add([]string{"c1", "c2", "c3"}, 1)
	c4.Add([]string{"c4", "c2", "c5"}, 1)
	assert.Equal(t, `{"all.c2.all": 2}`, c4.String())
}
