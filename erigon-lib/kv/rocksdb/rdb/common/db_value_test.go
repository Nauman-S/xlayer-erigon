package common

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSerialize(t *testing.T) {
	dbv := DBValue{values: [][]byte{
		[]byte("abcd"), []byte("123"), []byte("(xyz$"),
	}}

	data := dbv.Serialize()

	require.Equal(t,
		[]byte{
			3, 0, 0, 0,
			4, 0, 0, 0,
			3, 0, 0, 0,
			5, 0, 0, 0,
			'a', 'b', 'c', 'd',
			'1', '2', '3',
			'(', 'x', 'y', 'z', '$',
		}, data)
}

func TestDeserialize(t *testing.T) {
	buf := []byte{
		3, 0, 0, 0,
		4, 0, 0, 0,
		3, 0, 0, 0,
		5, 0, 0, 0,
		'a', 'b', 'c', 'd',
		'1', '2', '3',
		'(', 'x', 'y', 'z', '$',
	}

	dbv := DeserializeDBValue(buf)

	require.Equal(t, &DBValue{
		values: [][]byte{
			[]byte("abcd"),
			[]byte("123"),
			[]byte("(xyz$"),
		},
	}, dbv)
}

func TestSortedInsert(t *testing.T) {
	dbv := DBValue{values: [][]byte{}}

	dbv.SortedInsert([]byte("xbcd"))
	dbv.SortedInsert([]byte("gbcd"))
	dbv.SortedInsert([]byte("abcd"))
	dbv.SortedInsert([]byte("bbcd"))

	require.Equal(t, [][]byte{
		[]byte("abcd"),
		[]byte("bbcd"),
		[]byte("gbcd"),
		[]byte("xbcd"),
	}, dbv.values)
}
