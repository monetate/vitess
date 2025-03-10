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

package topo

import (
	"context"
	"fmt"
	"sync"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vterrors"

	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
)

// This file contains the utility methods to manage SrvVSchema objects.

// WatchSrvVSchemaData is returned / streamed by WatchSrvVSchema.
// The WatchSrvVSchema API guarantees exactly one of Value or Err will be set.
type WatchSrvVSchemaData struct {
	Value *vschemapb.SrvVSchema
	Err   error
}

// WatchSrvVSchema will set a watch on the SrvVSchema object.
// It has the same contract as Conn.Watch, but it also unpacks the
// contents into a SrvVSchema object.
func (ts *Server) WatchSrvVSchema(ctx context.Context, cell string) (*WatchSrvVSchemaData, <-chan *WatchSrvVSchemaData, error) {
	conn, err := ts.ConnForCell(ctx, cell)
	if err != nil {
		return nil, nil, err
	}

	ctx, cancel := context.WithCancel(ctx)
	current, wdChannel, err := conn.Watch(ctx, SrvVSchemaFile)
	if err != nil {
		cancel()
		return nil, nil, err
	}
	value := &vschemapb.SrvVSchema{}
	if err := value.UnmarshalVT(current.Contents); err != nil {
		// Cancel the watch, drain channel.
		cancel()
		for range wdChannel {
		}
		return nil, nil, vterrors.Wrapf(err, "error unpacking initial SrvVSchema object")
	}

	changes := make(chan *WatchSrvVSchemaData, 10)

	// The background routine reads any event from the watch channel,
	// translates it, and sends it to the caller.
	// If cancel() is called, the underlying Watch() code will
	// send an ErrInterrupted and then close the channel. We'll
	// just propagate that back to our caller.
	go func() {
		defer cancel()
		defer close(changes)

		for wd := range wdChannel {
			if wd.Err != nil {
				// Last error value, we're done.
				// wdChannel will be closed right after
				// this, no need to do anything.
				changes <- &WatchSrvVSchemaData{Err: wd.Err}
				return
			}

			value := &vschemapb.SrvVSchema{}
			if err := value.UnmarshalVT(wd.Contents); err != nil {
				cancel()
				for range wdChannel {
				}
				changes <- &WatchSrvVSchemaData{Err: vterrors.Wrapf(err, "error unpacking SrvVSchema object")}
				return
			}
			changes <- &WatchSrvVSchemaData{Value: value}
		}
	}()

	return &WatchSrvVSchemaData{Value: value}, changes, nil
}

// UpdateSrvVSchema updates the SrvVSchema file for a cell.
func (ts *Server) UpdateSrvVSchema(ctx context.Context, cell string, srvVSchema *vschemapb.SrvVSchema) error {
	conn, err := ts.ConnForCell(ctx, cell)
	if err != nil {
		return err
	}

	nodePath := SrvVSchemaFile
	data, err := srvVSchema.MarshalVT()
	if err != nil {
		return err
	}
	_, err = conn.Update(ctx, nodePath, data, nil)
	return err
}

// GetSrvVSchema returns the SrvVSchema for a cell.
func (ts *Server) GetSrvVSchema(ctx context.Context, cell string) (*vschemapb.SrvVSchema, error) {
	conn, err := ts.ConnForCell(ctx, cell)
	if err != nil {
		return nil, err
	}

	nodePath := SrvVSchemaFile
	data, _, err := conn.Get(ctx, nodePath)
	if err != nil {
		return nil, err
	}
	srvVSchema := &vschemapb.SrvVSchema{}
	if err := srvVSchema.UnmarshalVT(data); err != nil {
		return nil, vterrors.Wrapf(err, "SrvVSchema unmarshal failed: %v", data)
	}
	return srvVSchema, nil
}

// DeleteSrvVSchema deletes the SrvVSchema file for a cell.
func (ts *Server) DeleteSrvVSchema(ctx context.Context, cell string) error {
	conn, err := ts.ConnForCell(ctx, cell)
	if err != nil {
		return err
	}

	nodePath := SrvVSchemaFile
	return conn.Delete(ctx, nodePath, nil)
}

// RebuildSrvVSchema rebuilds the SrvVSchema for the provided cell list
// (or all cells if cell list is empty).
func (ts *Server) RebuildSrvVSchema(ctx context.Context, cells []string) error {
	// get the actual list of cells
	if len(cells) == 0 {
		var err error
		cells, err = ts.GetKnownCells(ctx)
		if err != nil {
			return fmt.Errorf("GetKnownCells failed: %v", err)
		}
	}

	// get the keyspaces
	keyspaces, err := ts.GetKeyspaces(ctx)
	if err != nil {
		return fmt.Errorf("GetKeyspaces failed: %v", err)
	}

	// build the SrvVSchema in parallel, protected by mu
	wg := sync.WaitGroup{}
	mu := sync.Mutex{}
	var finalErr error
	srvVSchema := &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{},
	}
	for _, keyspace := range keyspaces {
		wg.Add(1)
		go func(keyspace string) {
			defer wg.Done()

			ksvs, err := ts.GetVSchema(ctx, keyspace)
			if IsErrType(err, NoNode) {
				err = nil
				ksvs = &KeyspaceVSchemaInfo{
					Name:     keyspace,
					Keyspace: &vschemapb.Keyspace{},
				}
			}

			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				log.Errorf("%v: GetVSchema(%v) failed", err, keyspace)
				finalErr = err
				return
			}
			srvVSchema.Keyspaces[keyspace] = ksvs.Keyspace
		}(keyspace)
	}
	wg.Wait()
	if finalErr != nil {
		return finalErr
	}

	rr, err := ts.GetRoutingRules(ctx)
	if err != nil {
		return fmt.Errorf("GetRoutingRules failed: %v", err)
	}
	srvVSchema.RoutingRules = rr

	srr, err := ts.GetShardRoutingRules(ctx)
	if err != nil {
		return fmt.Errorf("GetShardRoutingRules failed: %v", err)
	}
	srvVSchema.ShardRoutingRules = srr

	krr, err := ts.GetKeyspaceRoutingRules(ctx)
	if err != nil {
		return fmt.Errorf("GetKeyspaceRoutingRules failed: %v", err)
	}
	srvVSchema.KeyspaceRoutingRules = krr

	mr, err := ts.GetMirrorRules(ctx)
	if err != nil {
		return fmt.Errorf("GetMirrorRules failed: %v", err)
	}
	srvVSchema.MirrorRules = mr

	// now save the SrvVSchema in all cells in parallel
	for _, cell := range cells {
		wg.Add(1)
		go func(cell string) {
			defer wg.Done()
			if err := ts.UpdateSrvVSchema(ctx, cell, srvVSchema); err != nil {
				log.Errorf("%v: UpdateSrvVSchema(%v) failed", err, cell)
				mu.Lock()
				finalErr = err
				mu.Unlock()
			}
		}(cell)
	}
	wg.Wait()

	return finalErr
}
