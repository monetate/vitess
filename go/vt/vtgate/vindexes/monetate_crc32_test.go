/*
Copyright 2023 Monetate, Inc.
*/

package vindexes

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
)

func TestMonetateCRC32Map(t *testing.T) {
	vindex, err := CreateVindex("monetate_crc32", "monetate", map[string]string{})
	require.NoError(t, err)
	mutiCol := vindex.(MultiColumn)

	got, err := mutiCol.Map(context.Background(), nil, [][]sqltypes.Value{{
		// visitor shard key
		sqltypes.NewInt64(1), sqltypes.NewInt64(2), sqltypes.NewInt64(3),
	}, {
		// customer shard key
		sqltypes.NewInt64(1), sqltypes.NewVarBinary("customer_id"),
	}, {
		// customer shard key, invalid customer id as null
		sqltypes.NewInt64(1), sqltypes.NULL,
	}})
	assert.NoError(t, err)

	want := []key.Destination{
		key.DestinationKeyspaceID("\x1b\x1c\x60\x00"),
		key.DestinationKeyspaceID("\xc4\x4b\xe0\x00"),
		key.DestinationNone{},
	}
	assert.Equal(t, want, got)
}
