/*
Copyright 2023 The Vitess Authors.

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

package engine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func TestDeleteWithInputSingleOffset(t *testing.T) {
	input := &fakePrimitive{results: []*sqltypes.Result{
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("id", "int64"), "1", "2", "3"),
	}}

	del := &DMLWithInput{
		Input: input,
		DMLs: []Primitive{&Delete{
			DML: &DML{
				RoutingParameters: &RoutingParameters{
					Opcode: Scatter,
					Keyspace: &vindexes.Keyspace{
						Name:    "ks",
						Sharded: true,
					},
				},
				Query: "dummy_delete",
			},
		}},
		OutputCols: [][]int{{0}},
	}

	vc := newTestVCursor("-20", "20-")
	_, err := del.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`InDMLExecution set to true`,
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ` +
			`ks.-20: dummy_delete {dml_vals: type:TUPLE values:{type:INT64 value:"1"} values:{type:INT64 value:"2"} values:{type:INT64 value:"3"}} ` +
			`ks.20-: dummy_delete {dml_vals: type:TUPLE values:{type:INT64 value:"1"} values:{type:INT64 value:"2"} values:{type:INT64 value:"3"}} true false`,
		`InDMLExecution set to false`,
	})

	vc.Rewind()
	input.rewind()
	err = del.TryStreamExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false, func(result *sqltypes.Result) error { return nil })
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`InDMLExecution set to true`,
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ` +
			`ks.-20: dummy_delete {dml_vals: type:TUPLE values:{type:INT64 value:"1"} values:{type:INT64 value:"2"} values:{type:INT64 value:"3"}} ` +
			`ks.20-: dummy_delete {dml_vals: type:TUPLE values:{type:INT64 value:"1"} values:{type:INT64 value:"2"} values:{type:INT64 value:"3"}} true false`,
		`InDMLExecution set to false`,
	})
}

func TestDeleteWithInputMultiOffset(t *testing.T) {
	input := &fakePrimitive{results: []*sqltypes.Result{
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("id|col", "int64|varchar"), "1|a", "2|b", "3|c"),
	}}

	del := &DMLWithInput{
		Input: input,
		DMLs: []Primitive{&Delete{
			DML: &DML{
				RoutingParameters: &RoutingParameters{
					Opcode: Scatter,
					Keyspace: &vindexes.Keyspace{
						Name:    "ks",
						Sharded: true,
					},
				},
				Query: "dummy_delete",
			},
		}},
		OutputCols: [][]int{{1, 0}},
	}

	vc := newTestVCursor("-20", "20-")
	_, err := del.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`InDMLExecution set to true`,
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ` +
			`ks.-20: dummy_delete {dml_vals: type:TUPLE values:{type:TUPLE value:"\x950\x01a\x89\x02\x011"} values:{type:TUPLE value:"\x950\x01b\x89\x02\x012"} values:{type:TUPLE value:"\x950\x01c\x89\x02\x013"}} ` +
			`ks.20-: dummy_delete {dml_vals: type:TUPLE values:{type:TUPLE value:"\x950\x01a\x89\x02\x011"} values:{type:TUPLE value:"\x950\x01b\x89\x02\x012"} values:{type:TUPLE value:"\x950\x01c\x89\x02\x013"}} true false`,
		`InDMLExecution set to false`,
	})

	vc.Rewind()
	input.rewind()
	err = del.TryStreamExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false, func(result *sqltypes.Result) error { return nil })
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`InDMLExecution set to true`,
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ` +
			`ks.-20: dummy_delete {dml_vals: type:TUPLE values:{type:TUPLE value:"\x950\x01a\x89\x02\x011"} values:{type:TUPLE value:"\x950\x01b\x89\x02\x012"} values:{type:TUPLE value:"\x950\x01c\x89\x02\x013"}} ` +
			`ks.20-: dummy_delete {dml_vals: type:TUPLE values:{type:TUPLE value:"\x950\x01a\x89\x02\x011"} values:{type:TUPLE value:"\x950\x01b\x89\x02\x012"} values:{type:TUPLE value:"\x950\x01c\x89\x02\x013"}} true false`,
		`InDMLExecution set to false`,
	})
}

func TestDeleteWithMultiTarget(t *testing.T) {
	input := &fakePrimitive{results: []*sqltypes.Result{
		sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("id|id|user_id", "int64|int64|int64"),
			"1|100|1", "2|100|2", "3|200|3"),
	}}

	vindex, _ := vindexes.CreateVindex("hash", "", nil)

	del1 := &Delete{
		DML: &DML{
			RoutingParameters: &RoutingParameters{
				Opcode:   IN,
				Keyspace: &vindexes.Keyspace{Name: "ks", Sharded: true},
				Vindex:   vindex,
				Values: []evalengine.Expr{
					&evalengine.BindVariable{Key: "dml_vals", Type: sqltypes.Tuple},
				},
			},
			Query: "dummy_delete_1",
		},
	}

	del2 := &Delete{
		DML: &DML{
			RoutingParameters: &RoutingParameters{
				Opcode:   MultiEqual,
				Keyspace: &vindexes.Keyspace{Name: "ks", Sharded: true},
				Vindex:   vindex,
				Values: []evalengine.Expr{
					&evalengine.TupleBindVariable{Key: "dml_vals", Index: 1},
				},
			},
			Query: "dummy_delete_2",
		},
	}

	del := &DMLWithInput{
		Input:      input,
		DMLs:       []Primitive{del1, del2},
		OutputCols: [][]int{{0}, {1, 2}},
	}

	vc := newTestVCursor("-20", "20-")
	_, err := del.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`InDMLExecution set to true`,
		`ResolveDestinations ks [type:INT64 value:"1" type:INT64 value:"2" type:INT64 value:"3"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6),DestinationKeyspaceID(06e7ea22ce92708f),DestinationKeyspaceID(4eb190c9a2fa169c)`,
		`ExecuteMultiShard ks.-20: dummy_delete_1 {__vals: type:TUPLE values:{type:INT64 value:"1"} values:{type:INT64 value:"2"} values:{type:INT64 value:"3"} dml_vals: type:TUPLE values:{type:INT64 value:"1"} values:{type:INT64 value:"2"} values:{type:INT64 value:"3"}} true true`,
		`ResolveDestinations ks [type:INT64 value:"1" type:INT64 value:"2" type:INT64 value:"3"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6),DestinationKeyspaceID(06e7ea22ce92708f),DestinationKeyspaceID(4eb190c9a2fa169c)`,
		`ExecuteMultiShard ks.-20: dummy_delete_2 {dml_vals: type:TUPLE values:{type:TUPLE value:"\x89\x02\x03100\x89\x02\x011"} values:{type:TUPLE value:"\x89\x02\x03100\x89\x02\x012"} values:{type:TUPLE value:"\x89\x02\x03200\x89\x02\x013"}} true true`,
		`InDMLExecution set to false`,
	})

	vc.Rewind()
	input.rewind()
	err = del.TryStreamExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false, func(result *sqltypes.Result) error { return nil })
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`InDMLExecution set to true`,
		`ResolveDestinations ks [type:INT64 value:"1" type:INT64 value:"2" type:INT64 value:"3"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6),DestinationKeyspaceID(06e7ea22ce92708f),DestinationKeyspaceID(4eb190c9a2fa169c)`,
		`ExecuteMultiShard ks.-20: dummy_delete_1 {__vals: type:TUPLE values:{type:INT64 value:"1"} values:{type:INT64 value:"2"} values:{type:INT64 value:"3"} dml_vals: type:TUPLE values:{type:INT64 value:"1"} values:{type:INT64 value:"2"} values:{type:INT64 value:"3"}} true true`,
		`ResolveDestinations ks [type:INT64 value:"1" type:INT64 value:"2" type:INT64 value:"3"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6),DestinationKeyspaceID(06e7ea22ce92708f),DestinationKeyspaceID(4eb190c9a2fa169c)`,
		`ExecuteMultiShard ks.-20: dummy_delete_2 {dml_vals: type:TUPLE values:{type:TUPLE value:"\x89\x02\x03100\x89\x02\x011"} values:{type:TUPLE value:"\x89\x02\x03100\x89\x02\x012"} values:{type:TUPLE value:"\x89\x02\x03200\x89\x02\x013"}} true true`,
		`InDMLExecution set to false`,
	})
}

// TestUpdateWithInputNonLiteral test the case where the column updated have non literal update.
// Therefore, update query should be executed for each row in the input result.
// This also validates the output rows affected.
func TestUpdateWithInputNonLiteral(t *testing.T) {
	input := &fakePrimitive{results: []*sqltypes.Result{
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("id|col|val", "int64|varchar|int64"), "1|a|100", "2|b|200", "3|c|300"),
	}}

	dml := &DMLWithInput{
		Input: input,
		DMLs: []Primitive{&Update{
			DML: &DML{
				RoutingParameters: &RoutingParameters{
					Opcode: Scatter,
					Keyspace: &vindexes.Keyspace{
						Name:    "ks",
						Sharded: true,
					},
				},
				Query: "dummy_update",
			},
		}},
		OutputCols: [][]int{{1, 0}},
		BVList: []map[string]int{
			{"bv1": 2},
		},
	}

	vc := newTestVCursor("-20", "20-")
	vc.results = []*sqltypes.Result{
		{RowsAffected: 1}, {RowsAffected: 1}, {RowsAffected: 1},
	}
	qr, err := dml.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`InDMLExecution set to true`,
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ` +
			`ks.-20: dummy_update {bv1: type:INT64 value:"100" dml_vals: type:TUPLE values:{type:TUPLE value:"\x950\x01a\x89\x02\x011"}} ` +
			`ks.20-: dummy_update {bv1: type:INT64 value:"100" dml_vals: type:TUPLE values:{type:TUPLE value:"\x950\x01a\x89\x02\x011"}} true false`,
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ` +
			`ks.-20: dummy_update {bv1: type:INT64 value:"200" dml_vals: type:TUPLE values:{type:TUPLE value:"\x950\x01b\x89\x02\x012"}} ` +
			`ks.20-: dummy_update {bv1: type:INT64 value:"200" dml_vals: type:TUPLE values:{type:TUPLE value:"\x950\x01b\x89\x02\x012"}} true false`,
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ` +
			`ks.-20: dummy_update {bv1: type:INT64 value:"300" dml_vals: type:TUPLE values:{type:TUPLE value:"\x950\x01c\x89\x02\x013"}} ` +
			`ks.20-: dummy_update {bv1: type:INT64 value:"300" dml_vals: type:TUPLE values:{type:TUPLE value:"\x950\x01c\x89\x02\x013"}} true false`,
		`InDMLExecution set to false`,
	})
	assert.EqualValues(t, 3, qr.RowsAffected)

	vc.Rewind()
	input.rewind()
	err = dml.TryStreamExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false,
		func(result *sqltypes.Result) error {
			qr = result
			return nil
		})
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`InDMLExecution set to true`,
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ` +
			`ks.-20: dummy_update {bv1: type:INT64 value:"100" dml_vals: type:TUPLE values:{type:TUPLE value:"\x950\x01a\x89\x02\x011"}} ` +
			`ks.20-: dummy_update {bv1: type:INT64 value:"100" dml_vals: type:TUPLE values:{type:TUPLE value:"\x950\x01a\x89\x02\x011"}} true false`,
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ` +
			`ks.-20: dummy_update {bv1: type:INT64 value:"200" dml_vals: type:TUPLE values:{type:TUPLE value:"\x950\x01b\x89\x02\x012"}} ` +
			`ks.20-: dummy_update {bv1: type:INT64 value:"200" dml_vals: type:TUPLE values:{type:TUPLE value:"\x950\x01b\x89\x02\x012"}} true false`,
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ` +
			`ks.-20: dummy_update {bv1: type:INT64 value:"300" dml_vals: type:TUPLE values:{type:TUPLE value:"\x950\x01c\x89\x02\x013"}} ` +
			`ks.20-: dummy_update {bv1: type:INT64 value:"300" dml_vals: type:TUPLE values:{type:TUPLE value:"\x950\x01c\x89\x02\x013"}} true false`,
		`InDMLExecution set to false`,
	})
	assert.EqualValues(t, 3, qr.RowsAffected)
}
