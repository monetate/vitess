/*
Copyright 2023 Monetate, Inc.
*/

package vindexes

import (
	"bytes"
	"context"
	"encoding/binary"
	"hash/crc32"
	"strings"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

var _ MultiColumn = (*MonetateCRC32)(nil)

type MonetateCRC32 struct {
	name string
}

// NewMonetateCRC32 creates a new MonetateCRC32.
func NewMonetateCRC32(name string, m map[string]string) (Vindex, error) {
	return &MonetateCRC32{name: name}, nil
}

func (m *MonetateCRC32) String() string {
	return m.name
}

func (m *MonetateCRC32) Cost() int {
	return 1
}

func (m *MonetateCRC32) IsUnique() bool {
	return true
}

func (m *MonetateCRC32) NeedsVCursor() bool {
	return false
}

func (m *MonetateCRC32) Map(ctx context.Context, vcursor VCursor, rowsColValues [][]sqltypes.Value) ([]key.Destination, error) {
	out := make([]key.Destination, 0, len(rowsColValues))
	for _, colValues := range rowsColValues {
		ksid, err := ChecksumValues(colValues)
		if err != nil {
			out = append(out, key.DestinationNone{})
			continue
		}
		out = append(out, key.DestinationKeyspaceID(ksid))
	}
	return out, nil
}

func (m *MonetateCRC32) Verify(ctx context.Context, vcursor VCursor, rowsColValues [][]sqltypes.Value, ksids [][]byte) ([]bool, error) {
	out := make([]bool, 0, len(rowsColValues))
	for idx, colValues := range rowsColValues {
		ksid, err := ChecksumValues(colValues)
		if err != nil {
			return nil, err
		}
		out = append(out, bytes.Equal(ksid, ksids[idx]))
	}
	return out, nil
}

func (m *MonetateCRC32) PartialVindex() bool {
	return false
}

func ChecksumValues(colValues []sqltypes.Value) ([]byte, error) {
	// concat string values of columns, separated by slashes
	var parts []string
	for _, colVal := range colValues {
		if !(colVal.IsIntegral() || colVal.IsText() || colVal.IsBinary()) {
			return nil, vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "invalid monetate vindex value")
		}
		parts = append(parts, colVal.ToString())
	}
	var shardKey = strings.Join(parts, "/")
	var checksum = crc32.ChecksumIEEE([]byte(shardKey))
	var vshard = checksum % 1048576 // 20 bit vshard id

	var hashed [4]byte
	binary.BigEndian.PutUint32(hashed[:], vshard<<12)
	return hashed[:], nil
}

func init() {
	Register("monetate_crc32", NewMonetateCRC32)
}
