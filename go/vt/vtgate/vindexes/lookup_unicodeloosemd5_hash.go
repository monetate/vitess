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

package vindexes

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
)

const (
	lookupUnicodeLooseMD5HashParamWriteOnly = "write_only"
)

var (
	_ SingleColumn    = (*LookupUnicodeLooseMD5Hash)(nil)
	_ Lookup          = (*LookupUnicodeLooseMD5Hash)(nil)
	_ ParamValidating = (*LookupUnicodeLooseMD5Hash)(nil)
	_ SingleColumn    = (*LookupUnicodeLooseMD5HashUnique)(nil)
	_ Lookup          = (*LookupUnicodeLooseMD5HashUnique)(nil)
	_ ParamValidating = (*LookupUnicodeLooseMD5HashUnique)(nil)

	lookupUnicodeLooseMD5HashParams = append(
		append(make([]string, 0), lookupCommonParams...),
		lookupUnicodeLooseMD5HashParamWriteOnly,
	)
)

func init() {
	Register("lookup_unicodeloosemd5_hash", newLookupUnicodeLooseMD5Hash)
	Register("lookup_unicodeloosemd5_hash_unique", newLookupUnicodeLooseMD5HashUnique)
}

// ====================================================================

// LookupUnicodeLooseMD5Hash defines a vindex that uses a lookup table.
// The table is expected to define the id column as unique. It's
// NonUnique and a Lookup and stores the from value in a hashed form.
// Warning: This Vindex is being deprecated in favor of Lookup
type LookupUnicodeLooseMD5Hash struct {
	name          string
	writeOnly     bool
	lkp           lookupInternal
	unknownParams []string
}

// newLookupUnicodeLooseMD5Hash creates a LookupUnicodeLooseMD5Hash vindex.
// The supplied map has the following required fields:
//
//	table: name of the backing table. It can be qualified by the keyspace.
//	from: list of columns in the table that have the 'from' values of the lookup vindex.
//	to: The 'to' column name of the table.
//
// The following fields are optional:
//
//	autocommit: setting this to "true" will cause inserts to upsert and deletes to be ignored.
//	write_only: in this mode, Map functions return the full keyrange causing a full scatter.
func newLookupUnicodeLooseMD5Hash(name string, m map[string]string) (Vindex, error) {
	lh := &LookupUnicodeLooseMD5Hash{
		name:          name,
		unknownParams: FindUnknownParams(m, lookupUnicodeLooseMD5HashParams),
	}

	cc, err := parseCommonConfig(m)
	if err != nil {
		return nil, err
	}
	lh.writeOnly, err = boolFromMap(m, lookupUnicodeLooseMD5HashParamWriteOnly)
	if err != nil {
		return nil, err
	}

	// if autocommit is on for non-unique lookup, upsert should also be on.
	if err := lh.lkp.Init(m, cc.autocommit, cc.autocommit || cc.multiShardAutocommit, cc.multiShardAutocommit); err != nil {
		return nil, err
	}
	return lh, nil
}

// String returns the name of the vindex.
func (lh *LookupUnicodeLooseMD5Hash) String() string {
	return lh.name
}

// Cost returns the cost of this vindex as 20.
func (lh *LookupUnicodeLooseMD5Hash) Cost() int {
	return 20
}

// IsUnique returns false since the Vindex is not unique.
func (lh *LookupUnicodeLooseMD5Hash) IsUnique() bool {
	return false
}

// NeedsVCursor satisfies the Vindex interface.
func (lh *LookupUnicodeLooseMD5Hash) NeedsVCursor() bool {
	return true
}

// Map can map ids to key.ShardDestination objects.
func (lh *LookupUnicodeLooseMD5Hash) Map(ctx context.Context, vcursor VCursor, ids []sqltypes.Value) ([]key.ShardDestination, error) {
	out := make([]key.ShardDestination, 0, len(ids))
	if lh.writeOnly {
		for range ids {
			out = append(out, key.DestinationKeyRange{KeyRange: &topodatapb.KeyRange{}})
		}
		return out, nil
	}

	// if ignore_nulls is set and the query is about single null value, then fallback to all shards
	if len(ids) == 1 && ids[0].IsNull() && lh.lkp.IgnoreNulls {
		for range ids {
			out = append(out, key.DestinationKeyRange{KeyRange: &topodatapb.KeyRange{}})
		}
		return out, nil
	}

	ids, err := convertIds(ids)
	if err != nil {
		return nil, err
	}
	results, err := lh.lkp.Lookup(ctx, vcursor, ids, vtgatepb.CommitOrder_NORMAL)
	if err != nil {
		return nil, err
	}
	for _, result := range results {
		if len(result.Rows) == 0 {
			out = append(out, key.DestinationNone{})
			continue
		}
		ksids := make([][]byte, 0, len(result.Rows))
		for _, row := range result.Rows {
			num, err := row[0].ToCastUint64()
			if err != nil {
				// A failure to convert is equivalent to not being
				// able to map.
				continue
			}
			ksids = append(ksids, vhash(num))
		}
		out = append(out, key.DestinationKeyspaceIDs(ksids))
	}
	return out, nil
}

func (lh *LookupUnicodeLooseMD5Hash) AutoCommitEnabled() bool {
	return lh.lkp.Autocommit
}

// Verify returns true if ids maps to ksids.
func (lh *LookupUnicodeLooseMD5Hash) Verify(ctx context.Context, vcursor VCursor, ids []sqltypes.Value, ksids [][]byte) ([]bool, error) {
	if lh.writeOnly {
		out := make([]bool, len(ids))
		for i := range ids {
			out[i] = true
		}
		return out, nil
	}

	values, err := unhashList(ksids)
	if err != nil {
		return nil, fmt.Errorf("lookup.Verify.vunhash: %v", err)
	}
	ids, err = convertIds(ids)
	if err != nil {
		return nil, fmt.Errorf("lookup.Verify.vunhash: %v", err)
	}
	return lh.lkp.Verify(ctx, vcursor, ids, values)
}

// Create reserves the id by inserting it into the vindex table.
func (lh *LookupUnicodeLooseMD5Hash) Create(ctx context.Context, vcursor VCursor, rowsColValues [][]sqltypes.Value, ksids [][]byte, ignoreMode bool) error {
	values, err := unhashList(ksids)
	if err != nil {
		return fmt.Errorf("lookup.Create.vunhash: %v", err)
	}
	rowsColValues, err = convertRows(rowsColValues)
	if err != nil {
		return fmt.Errorf("lookup.Create.convert: %v", err)
	}
	return lh.lkp.Create(ctx, vcursor, rowsColValues, values, ignoreMode)
}

// Update updates the entry in the vindex table.
func (lh *LookupUnicodeLooseMD5Hash) Update(ctx context.Context, vcursor VCursor, oldValues []sqltypes.Value, ksid []byte, newValues []sqltypes.Value) error {
	v, err := vunhash(ksid)
	if err != nil {
		return fmt.Errorf("lookup.Update.vunhash: %v", err)
	}
	newValues, err = convertIds(newValues)
	if err != nil {
		return fmt.Errorf("lookup.Update.convert: %v", err)
	}
	oldValues, err = convertIds(oldValues)
	if err != nil {
		return fmt.Errorf("lookup.Update.convert: %v", err)
	}
	return lh.lkp.Update(ctx, vcursor, oldValues, ksid, sqltypes.NewUint64(v), newValues)
}

// Delete deletes the entry from the vindex table.
func (lh *LookupUnicodeLooseMD5Hash) Delete(ctx context.Context, vcursor VCursor, rowsColValues [][]sqltypes.Value, ksid []byte) error {
	v, err := vunhash(ksid)
	if err != nil {
		return fmt.Errorf("lookup.Delete.vunhash: %v", err)
	}
	rowsColValues, err = convertRows(rowsColValues)
	if err != nil {
		return fmt.Errorf("lookup.Delete.convert: %v", err)
	}
	return lh.lkp.Delete(ctx, vcursor, rowsColValues, sqltypes.NewUint64(v), vtgatepb.CommitOrder_NORMAL)
}

// MarshalJSON returns a JSON representation of LookupHash.
func (lh *LookupUnicodeLooseMD5Hash) MarshalJSON() ([]byte, error) {
	return json.Marshal(lh.lkp)
}

// UnknownParams implements the ParamValidating interface.
func (lh *LookupUnicodeLooseMD5Hash) UnknownParams() []string {
	return lh.unknownParams
}

// ====================================================================

// LookupUnicodeLooseMD5HashUnique defines a vindex that uses a lookup table.
// The table is expected to define the id column as unique. It's
// Unique and a Lookup and will store the from value in a hashed format.
// Warning: This Vindex is being deprecated in favor of LookupUnique
type LookupUnicodeLooseMD5HashUnique struct {
	name          string
	writeOnly     bool
	lkp           lookupInternal
	unknownParams []string
}

// newLookupUnicodeLooseMD5HashUnique creates a LookupUnicodeLooseMD5HashUnique vindex.
// The supplied map has the following required fields:
//
//	table: name of the backing table. It can be qualified by the keyspace.
//	from: list of columns in the table that have the 'from' values of the lookup vindex.
//	to: The 'to' column name of the table.
//
// The following fields are optional:
//
//	autocommit: setting this to "true" will cause deletes to be ignored.
//	write_only: in this mode, Map functions return the full keyrange causing a full scatter.
func newLookupUnicodeLooseMD5HashUnique(name string, m map[string]string) (Vindex, error) {
	lhu := &LookupUnicodeLooseMD5HashUnique{
		name:          name,
		unknownParams: FindUnknownParams(m, lookupUnicodeLooseMD5HashParams),
	}

	cc, err := parseCommonConfig(m)
	if err != nil {
		return nil, err
	}
	lhu.writeOnly, err = boolFromMap(m, lookupUnicodeLooseMD5HashParamWriteOnly)
	if err != nil {
		return nil, err
	}

	// Don't allow upserts for unique vindexes.
	if err := lhu.lkp.Init(m, cc.autocommit, false /* upsert */, cc.multiShardAutocommit); err != nil {
		return nil, err
	}
	return lhu, nil
}

// String returns the name of the vindex.
func (lhu *LookupUnicodeLooseMD5HashUnique) String() string {
	return lhu.name
}

// Cost returns the cost of this vindex as 10.
func (lhu *LookupUnicodeLooseMD5HashUnique) Cost() int {
	return 10
}

// IsUnique returns true since the Vindex is unique.
func (lhu *LookupUnicodeLooseMD5HashUnique) IsUnique() bool {
	return true
}

// NeedsVCursor satisfies the Vindex interface.
func (lhu *LookupUnicodeLooseMD5HashUnique) NeedsVCursor() bool {
	return true
}

// Map can map ids to key.ShardDestination objects.
func (lhu *LookupUnicodeLooseMD5HashUnique) Map(ctx context.Context, vcursor VCursor, ids []sqltypes.Value) ([]key.ShardDestination, error) {
	out := make([]key.ShardDestination, 0, len(ids))
	if lhu.writeOnly {
		for range ids {
			out = append(out, key.DestinationKeyRange{KeyRange: &topodatapb.KeyRange{}})
		}
		return out, nil
	}

	ids, err := convertIds(ids)
	if err != nil {
		return nil, err
	}
	results, err := lhu.lkp.Lookup(ctx, vcursor, ids, vtgatepb.CommitOrder_NORMAL)
	if err != nil {
		return nil, err
	}
	for i, result := range results {
		switch len(result.Rows) {
		case 0:
			out = append(out, key.DestinationNone{})
		case 1:
			num, err := result.Rows[0][0].ToCastUint64()
			if err != nil {
				out = append(out, key.DestinationNone{})
				continue
			}
			out = append(out, key.DestinationKeyspaceID(vhash(num)))
		default:
			return nil, fmt.Errorf("LookupUnicodeLooseMD5HashUnique.Map: unexpected multiple results from vindex %s: %v", lhu.lkp.Table, ids[i])
		}
	}
	return out, nil
}

func (lhu *LookupUnicodeLooseMD5HashUnique) AutoCommitEnabled() bool {
	return lhu.lkp.Autocommit
}

// Verify returns true if ids maps to ksids.
func (lhu *LookupUnicodeLooseMD5HashUnique) Verify(ctx context.Context, vcursor VCursor, ids []sqltypes.Value, ksids [][]byte) ([]bool, error) {
	if lhu.writeOnly {
		out := make([]bool, len(ids))
		for i := range ids {
			out[i] = true
		}
		return out, nil
	}

	values, err := unhashList(ksids)
	if err != nil {
		return nil, fmt.Errorf("lookup.Verify.vunhash: %v", err)
	}
	ids, err = convertIds(ids)
	if err != nil {
		return nil, fmt.Errorf("lookup.Verify.vunhash: %v", err)
	}
	return lhu.lkp.Verify(ctx, vcursor, ids, values)
}

// Create reserves the id by inserting it into the vindex table.
func (lhu *LookupUnicodeLooseMD5HashUnique) Create(ctx context.Context, vcursor VCursor, rowsColValues [][]sqltypes.Value, ksids [][]byte, ignoreMode bool) error {
	values, err := unhashList(ksids)
	if err != nil {
		return fmt.Errorf("lookup.Create.vunhash: %v", err)
	}
	rowsColValues, err = convertRows(rowsColValues)
	if err != nil {
		return fmt.Errorf("lookup.Create.convert: %v", err)
	}
	return lhu.lkp.Create(ctx, vcursor, rowsColValues, values, ignoreMode)
}

// Delete deletes the entry from the vindex table.
func (lhu *LookupUnicodeLooseMD5HashUnique) Delete(ctx context.Context, vcursor VCursor, rowsColValues [][]sqltypes.Value, ksid []byte) error {
	v, err := vunhash(ksid)
	if err != nil {
		return fmt.Errorf("lookup.Delete.vunhash: %v", err)
	}
	rowsColValues, err = convertRows(rowsColValues)
	if err != nil {
		return fmt.Errorf("lookup.Delete.convert: %v", err)
	}
	return lhu.lkp.Delete(ctx, vcursor, rowsColValues, sqltypes.NewUint64(v), vtgatepb.CommitOrder_NORMAL)
}

// Update updates the entry in the vindex table.
func (lhu *LookupUnicodeLooseMD5HashUnique) Update(ctx context.Context, vcursor VCursor, oldValues []sqltypes.Value, ksid []byte, newValues []sqltypes.Value) error {
	v, err := vunhash(ksid)
	if err != nil {
		return fmt.Errorf("lookup.Update.vunhash: %v", err)
	}
	newValues, err = convertIds(newValues)
	if err != nil {
		return fmt.Errorf("lookup.Update.convert: %v", err)
	}
	oldValues, err = convertIds(oldValues)
	if err != nil {
		return fmt.Errorf("lookup.Update.convert: %v", err)
	}
	return lhu.lkp.Update(ctx, vcursor, oldValues, ksid, sqltypes.NewUint64(v), newValues)
}

// MarshalJSON returns a JSON representation of LookupHashUnique.
func (lhu *LookupUnicodeLooseMD5HashUnique) MarshalJSON() ([]byte, error) {
	return json.Marshal(lhu.lkp)
}

// IsBackfilling implements the LookupBackfill interface
func (lhu *LookupUnicodeLooseMD5HashUnique) IsBackfilling() bool {
	return lhu.writeOnly
}

// UnknownParams implements the ParamValidating interface.
func (lhu *LookupUnicodeLooseMD5HashUnique) UnknownParams() []string {
	return lhu.unknownParams
}

func unicodeHashValue(value sqltypes.Value) (sqltypes.Value, error) {
	hash, err := unicodeHash(&collateMD5, value)
	if err != nil {
		return sqltypes.NULL, err
	}

	return sqltypes.NewUint64(binary.BigEndian.Uint64(hash[:8])), nil
}

func convertIds(ids []sqltypes.Value) ([]sqltypes.Value, error) {
	converted := make([]sqltypes.Value, 0, len(ids))
	for _, id := range ids {
		idVal, err := unicodeHashValue(id)
		if err != nil {
			return nil, err
		}
		converted = append(converted, idVal)
	}
	return converted, nil
}

func convertRows(rows [][]sqltypes.Value) ([][]sqltypes.Value, error) {
	converted := make([][]sqltypes.Value, 0, len(rows))
	for _, row := range rows {
		row, err := convertIds(row)
		if err != nil {
			return nil, err
		}
		converted = append(converted, row)
	}
	return converted, nil
}
