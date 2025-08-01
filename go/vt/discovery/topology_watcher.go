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

package discovery

import (
	"bytes"
	"context"
	"fmt"
	"hash/crc32"
	"sort"
	"strings"
	"sync"
	"time"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/logutil"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
)

const (
	topologyWatcherOpListTablets   = "ListTablets"
	topologyWatcherOpGetTablet     = "GetTablet"
	topologyWatcherOpAddTablet     = "AddTablet"
	topologyWatcherOpRemoveTablet  = "RemoveTablet"
	topologyWatcherOpReplaceTablet = "ReplaceTablet"
)

var (
	topologyWatcherOperations = stats.NewCountersWithSingleLabel("TopologyWatcherOperations", "Topology watcher operation counts",
		"Operation", topologyWatcherOpListTablets, topologyWatcherOpGetTablet, topologyWatcherOpAddTablet, topologyWatcherOpRemoveTablet, topologyWatcherOpReplaceTablet)
	topologyWatcherErrors = stats.NewCountersWithSingleLabel("TopologyWatcherErrors", "Topology watcher error counts",
		"Operation", topologyWatcherOpListTablets, topologyWatcherOpGetTablet)
)

// tabletInfo is used internally by the TopologyWatcher struct.
type tabletInfo struct {
	alias  string
	tablet *topodatapb.Tablet
}

// TopologyWatcher polls the topology periodically for changes to
// the set of tablets. When tablets are added / removed / modified,
// it calls the AddTablet / RemoveTablet interface appropriately.
type TopologyWatcher struct {
	// set at construction time
	topoServer          *topo.Server
	healthcheck         HealthCheck
	tabletFilter        TabletFilter
	cell                string
	refreshInterval     time.Duration
	refreshKnownTablets bool
	ctx                 context.Context
	cancelFunc          context.CancelFunc
	// wg keeps track of all launched Go routines.
	wg sync.WaitGroup

	// mu protects all variables below
	mu sync.Mutex
	// tablets contains a map of alias -> tabletInfo for all known tablets.
	tablets map[string]*tabletInfo
	// topoChecksum stores a crc32 of the tablets map and is exported as a metric.
	topoChecksum uint32
	// lastRefresh records the timestamp of the last refresh of the topology.
	lastRefresh time.Time
	// firstLoadDone is true when the initial load of the topology data is complete.
	firstLoadDone bool
	// firstLoadChan is closed when the initial load of topology data is complete.
	firstLoadChan chan struct{}
	// options contains optional settings used to modify HealthCheckImpl
	// behavior.
	options Options
}

// NewTopologyWatcher returns a TopologyWatcher that monitors all
// the tablets in a cell, and reloads them as needed.
func NewTopologyWatcher(
	ctx context.Context, topoServer *topo.Server, hc HealthCheck, filter TabletFilter, cell string, refreshInterval time.Duration, refreshKnownTablets bool, opts ...Option,
) *TopologyWatcher {
	tw := &TopologyWatcher{
		topoServer:          topoServer,
		healthcheck:         hc,
		tabletFilter:        filter,
		cell:                cell,
		refreshInterval:     refreshInterval,
		refreshKnownTablets: refreshKnownTablets,
		tablets:             make(map[string]*tabletInfo),
		options:             withOptions(opts...),
	}
	tw.firstLoadChan = make(chan struct{})

	// We want the span from the context, but not the cancellation that comes with it
	spanContext := trace.CopySpan(context.Background(), ctx)
	tw.ctx, tw.cancelFunc = context.WithCancel(spanContext)
	return tw
}

func (tw *TopologyWatcher) getTablets() ([]*topo.TabletInfo, error) {
	return tw.topoServer.GetTabletsByCell(tw.ctx, tw.cell, nil)
}

func (tw *TopologyWatcher) getTabletsByShard(keyspace string, shard string) ([]*topo.TabletInfo, error) {
	return tw.topoServer.GetTabletsByShardCell(tw.ctx, keyspace, shard, []string{tw.cell})
}

// Start starts the topology watcher.
func (tw *TopologyWatcher) Start() {
	tw.wg.Add(1)
	// Goroutine to refresh the tablets list periodically.
	go func(t *TopologyWatcher) {
		defer t.wg.Done()
		ticker := time.NewTicker(t.refreshInterval)
		defer ticker.Stop()
		t.loadTablets()
		for {
			select {
			case <-t.ctx.Done():
				return
			case kss := <-t.healthcheck.GetLoadTabletsTrigger():
				t.loadTabletsForKeyspaceShard(kss.Keyspace, kss.Shard)
			case <-ticker.C:
				// Since we are going to load all the tablets,
				// we can clear out the entire list for reloading
				// specific keyspace shards.
				func() {
					for {
						select {
						case <-t.healthcheck.GetLoadTabletsTrigger():
						default:
							return
						}
					}
				}()
				t.loadTablets()
			}
		}
	}(tw)
}

// Stop stops the watcher. It does not clean up the tablets added to HealthCheck.
func (tw *TopologyWatcher) Stop() {
	tw.cancelFunc()
	// wait for watch goroutine to finish.
	tw.wg.Wait()
}

func (tw *TopologyWatcher) loadTabletsForKeyspaceShard(keyspace string, shard string) {
	if keyspace == "" || shard == "" {
		tw.logger().Errorf("topologyWatcher: loadTabletsForKeyspaceShard: keyspace and shard are required")
		return
	}
	tabletInfos, err := tw.getTabletsByShard(keyspace, shard)
	if err != nil {
		tw.logger().Errorf("error getting tablets for keyspace-shard: %v:%v: %v", keyspace, shard, err)
		return
	}
	// Since we are only reading tablets for a keyspace shard,
	// this is by default a partial result.
	tw.storeTabletInfos(tabletInfos /* partialResults */, true)
}

func (tw *TopologyWatcher) loadTablets() {
	var partialResult bool
	// First get the list of all tablets.
	tabletInfos, err := tw.getTablets()
	topologyWatcherOperations.Add(topologyWatcherOpListTablets, 1)
	if err != nil {
		topologyWatcherErrors.Add(topologyWatcherOpListTablets, 1)
		// If we get a partial result error, we just log it and process the tablets that we did manage to fetch.
		if topo.IsErrType(err, topo.PartialResult) {
			tw.logger().Errorf("received partial result from getTablets for cell %v: %v", tw.cell, err)
			partialResult = true
		} else { // For all other errors, just return.
			tw.logger().Errorf("error getting tablets for cell: %v: %v", tw.cell, err)
			return
		}
	}

	tw.storeTabletInfos(tabletInfos, partialResult)
}

func (tw *TopologyWatcher) storeTabletInfos(tabletInfos []*topo.TabletInfo, partialResult bool) {
	newTablets := make(map[string]*tabletInfo)
	// Accumulate a list of all known alias strings to use later
	// when sorting.
	tabletAliasStrs := make([]string, 0, len(tabletInfos))

	tw.mu.Lock()
	defer tw.mu.Unlock()

	for _, tInfo := range tabletInfos {
		aliasStr := topoproto.TabletAliasString(tInfo.Alias)
		tabletAliasStrs = append(tabletAliasStrs, aliasStr)

		if !tw.refreshKnownTablets {
			// We already have a tabletInfo for this and the flag tells us to not refresh.
			if val, ok := tw.tablets[aliasStr]; ok {
				newTablets[aliasStr] = val
				continue
			}
		}
		// There's no network call here, so we just do the tablets one at a time instead of in parallel goroutines.
		newTablets[aliasStr] = &tabletInfo{
			alias:  aliasStr,
			tablet: tInfo.Tablet,
		}
	}

	if partialResult {
		// We don't want to remove any tablets from the tablets map or the healthcheck if we got a partial result
		// because we don't know if they were actually deleted or if we simply failed to fetch them.
		// Fill any gaps in the newTablets map using the existing tablets.
		for alias, val := range tw.tablets {
			if _, ok := newTablets[alias]; !ok {
				tabletAliasStrs = append(tabletAliasStrs, alias)
				newTablets[alias] = val
			}
		}
	}

	for alias, newVal := range newTablets {
		if tw.tabletFilter != nil && !tw.tabletFilter.IsIncluded(newVal.tablet) {
			continue
		}

		// Trust the alias from topo and add it if it doesn't exist.
		if val, ok := tw.tablets[alias]; ok {
			// check if the host and port have changed. If yes, replace tablet.
			oldKey := TabletToMapKey(val.tablet)
			newKey := TabletToMapKey(newVal.tablet)
			if oldKey != newKey {
				// This is the case where the same tablet alias is now reporting
				// a different address (host:port) key.
				tw.healthcheck.ReplaceTablet(val.tablet, newVal.tablet)
				topologyWatcherOperations.Add(topologyWatcherOpReplaceTablet, 1)
			}
		} else {
			// This is a new tablet record, let's add it to the HealthCheck.
			tw.healthcheck.AddTablet(newVal.tablet)
			topologyWatcherOperations.Add(topologyWatcherOpAddTablet, 1)
		}
	}

	for _, val := range tw.tablets {
		if tw.tabletFilter != nil && !tw.tabletFilter.IsIncluded(val.tablet) {
			continue
		}

		if _, ok := newTablets[val.alias]; !ok {
			tw.healthcheck.RemoveTablet(val.tablet)
			topologyWatcherOperations.Add(topologyWatcherOpRemoveTablet, 1)
		}
	}
	tw.tablets = newTablets
	if !tw.firstLoadDone {
		tw.firstLoadDone = true
		close(tw.firstLoadChan)
	}

	// Iterate through the tablets in a stable order and compute a
	// checksum of the tablet map.
	sort.Strings(tabletAliasStrs)
	var buf bytes.Buffer
	for _, alias := range tabletAliasStrs {
		_, ok := tw.tablets[alias]
		if ok {
			buf.WriteString(alias)
		}
	}
	tw.topoChecksum = crc32.ChecksumIEEE(buf.Bytes())
	tw.lastRefresh = time.Now()
}

// RefreshLag returns the time since the last refresh.
func (tw *TopologyWatcher) RefreshLag() time.Duration {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	return time.Since(tw.lastRefresh)
}

// TopoChecksum returns the checksum of the current state of the topo.
func (tw *TopologyWatcher) TopoChecksum() uint32 {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	return tw.topoChecksum
}

// logger returns the logutil.Logger used by the TopologyWatcher.
func (tw *TopologyWatcher) logger() logutil.Logger {
	return tw.options.logger
}

// TabletFilter is an interface that can be given to a TopologyWatcher
// to be applied as an additional filter on the list of tablets returned by its getTablets function.
type TabletFilter interface {
	// IsIncluded returns whether tablet is included in this filter
	IsIncluded(tablet *topodatapb.Tablet) bool
}

// TabletFilters contains filters for tablets.
type TabletFilters []TabletFilter

// IsIncluded returns true if a tablet passes all filters.
func (tf TabletFilters) IsIncluded(tablet *topodatapb.Tablet) bool {
	for _, filter := range tf {
		if !filter.IsIncluded(tablet) {
			return false
		}
	}
	return true
}

// FilterByShard is a filter that filters tablets by
// keyspace/shard.
type FilterByShard struct {
	// filters is a map of keyspace to filters for shards
	filters map[string][]*filterShard
	// options contains optional settings used to modify FilterByShard
	// behavior.
	options Options
}

// filterShard describes a filter for a given shard or keyrange inside
// a keyspace.
type filterShard struct {
	keyspace string
	shard    string
	keyRange *topodatapb.KeyRange // only set if shard is also a KeyRange
	options  Options
}

// NewFilterByShard creates a new FilterByShard for use by a
// TopologyWatcher. Each filter is a keyspace|shard entry, where shard
// can either be a shard name, or a keyrange. All tablets that match
// at least one keyspace|shard tuple will be forwarded by the
// TopologyWatcher to its consumer.
func NewFilterByShard(filters []string, opts ...Option) (*FilterByShard, error) {
	m := make(map[string][]*filterShard)
	for _, filter := range filters {
		parts := strings.Split(filter, "|")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid FilterByShard parameter: %v", filter)
		}

		keyspace := parts[0]
		shard := parts[1]

		// extract keyrange if it's a range
		canonical, kr, err := topo.ValidateShardName(shard)
		if err != nil {
			return nil, fmt.Errorf("error parsing shard name %v: %v", shard, err)
		}

		// check for duplicates
		for _, c := range m[keyspace] {
			if c.shard == canonical {
				return nil, fmt.Errorf("duplicate %v/%v entry", keyspace, shard)
			}
		}

		m[keyspace] = append(m[keyspace], &filterShard{
			keyspace: keyspace,
			shard:    canonical,
			keyRange: kr,
		})
	}

	fbs := &FilterByShard{
		filters: m,
		options: withOptions(opts...),
	}

	return fbs, nil
}

// IsIncluded returns true iff the tablet's keyspace and shard match what we have.
func (fbs *FilterByShard) IsIncluded(tablet *topodatapb.Tablet) bool {
	canonical, kr, err := topo.ValidateShardName(tablet.Shard)
	if err != nil {
		fbs.logger().Errorf("Error parsing shard name %v, will ignore tablet: %v", tablet.Shard, err)
		return false
	}

	for _, c := range fbs.filters[tablet.Keyspace] {
		if canonical == c.shard {
			// Exact match (probably a non-sharded keyspace).
			return true
		}
		if kr != nil && c.keyRange != nil && key.KeyRangeContainsKeyRange(c.keyRange, kr) {
			// Our filter's KeyRange includes the provided KeyRange
			return true
		}
	}
	return false
}

// logger returns the logutil.Logger used by the FilterByShard.
func (fbs *FilterByShard) logger() logutil.Logger {
	return fbs.options.logger
}

// FilterByKeyspace is a filter that filters tablets by keyspace.
type FilterByKeyspace struct {
	keyspaces map[string]bool
}

// NewFilterByKeyspace creates a new FilterByKeyspace.
// Each filter is a keyspace entry. All tablets that match
// a keyspace will be forwarded to the TopologyWatcher's consumer.
func NewFilterByKeyspace(selectedKeyspaces []string) *FilterByKeyspace {
	m := make(map[string]bool)
	for _, keyspace := range selectedKeyspaces {
		m[keyspace] = true
	}

	return &FilterByKeyspace{
		keyspaces: m,
	}
}

// IsIncluded returns true if the tablet's keyspace matches what we have.
func (fbk *FilterByKeyspace) IsIncluded(tablet *topodatapb.Tablet) bool {
	_, exist := fbk.keyspaces[tablet.Keyspace]
	return exist
}

// FilterByTabletTags is a filter that filters tablets by tablet tag key/values.
type FilterByTabletTags struct {
	tags map[string]string
}

// NewFilterByTabletTags creates a new FilterByTabletTags. All tablets that match
// all tablet tags will be forwarded to the TopologyWatcher's consumer.
func NewFilterByTabletTags(tabletTags map[string]string) *FilterByTabletTags {
	return &FilterByTabletTags{
		tags: tabletTags,
	}
}

// IsIncluded returns true if the tablet's tags match what we expect.
func (fbtg *FilterByTabletTags) IsIncluded(tablet *topodatapb.Tablet) bool {
	if fbtg.tags == nil {
		return true
	}
	if tablet.Tags == nil {
		return false
	}
	for key, val := range fbtg.tags {
		if tabletVal, found := tablet.Tags[key]; !found || tabletVal != val {
			return false
		}
	}
	return true
}
