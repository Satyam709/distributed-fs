package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetDirForChunkId(t *testing.T) {
	testCases := []struct {
		name           string
		chunkId        string
		shardLvl       uint16
		expectedResult string
		expectError    error
	}{
		{
			name:           "a valid case",
			chunkId:        "abcdefgh....",
			shardLvl:       2,
			expectedResult: "ab/cd/",
		},
		{
			name:           "a invalid case 1",
			chunkId:        "ab",
			shardLvl:       2,
			expectedResult: "",
			expectError:    InvalidChunkId,
		},
		{
			name:           "a invalid case 2",
			chunkId:        "abc",
			shardLvl:       2,
			expectedResult: "",
			expectError:    InvalidChunkId,
		},
		{
			name:           "a valid case",
			chunkId:        "abcd",
			shardLvl:       2,
			expectedResult: "ab/cd/",
		},
		{
			name:           "a large sharding",
			chunkId:        "abcdefghsfsadw....",
			shardLvl:       5,
			expectedResult: "ab/cd/ef/gh/sf/",
		},
	}
	for _, tC := range testCases {
		t.Run(tC.name, func(t *testing.T) {
			res, err := getDirForChunkId(tC.chunkId, tC.shardLvl)
			assert.ErrorIs(t, err, tC.expectError, "err dont match")
			assert.Equal(t, tC.expectedResult, res, "result not match")
		})
	}
}
