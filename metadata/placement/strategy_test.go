package placement

import (
	"sort"
	"testing"

	"github.com/satyam709/distributed-fs/metadata/fsm"
	"github.com/stretchr/testify/require"
)

func TestSortByMostFreeSpace(t *testing.T) {
	nodes := []fsm.NodeEntry{
		{NodeID: "n1", FreeSpace: 100, ChunkCount: 5},
		{NodeID: "n2", FreeSpace: 500, ChunkCount: 2},
		{NodeID: "n3", FreeSpace: 300, ChunkCount: 10},
		{NodeID: "n4", FreeSpace: 500, ChunkCount: 1},
	}

	sorted := SortByMostFreeSpace(nodes)
	sort.Sort(sorted)

	require.Equal(t, "n4", sorted[0].NodeID)
	require.Equal(t, "n2", sorted[1].NodeID)
	require.Equal(t, "n3", sorted[2].NodeID)
	require.Equal(t, "n1", sorted[3].NodeID)
}

func TestSortByMostFreeSpace_SameFreeSpace(t *testing.T) {
	nodes := []fsm.NodeEntry{
		{NodeID: "n1", FreeSpace: 100, ChunkCount: 10},
		{NodeID: "n2", FreeSpace: 100, ChunkCount: 5},
		{NodeID: "n3", FreeSpace: 100, ChunkCount: 20},
	}

	sorted := SortByMostFreeSpace(nodes)
	sort.Sort(sorted)

	require.Equal(t, "n2", sorted[0].NodeID)
	require.Equal(t, "n1", sorted[1].NodeID)
	require.Equal(t, "n3", sorted[2].NodeID)
}

func TestMostFreeSpaceStrategy_SelectNodes_Success(t *testing.T) {
	nodes := []fsm.NodeEntry{
		{NodeID: "n1", FreeSpace: 100},
		{NodeID: "n2", FreeSpace: 500},
		{NodeID: "n3", FreeSpace: 300},
	}

	strategy := MostFreeSpaceStrategy{Nodes: nodes}

	selected, err := strategy.SelectNodes("chunk1", 2)
	require.NoError(t, err)
	require.Len(t, selected, 2)
	require.Equal(t, "n2", selected[0].NodeID)
	require.Equal(t, "n3", selected[1].NodeID)
}

func TestMostFreeSpaceStrategy_SelectNodes_InsufficientNodes(t *testing.T) {
	nodes := []fsm.NodeEntry{
		{NodeID: "n1", FreeSpace: 100},
		{NodeID: "n2", FreeSpace: 500},
	}

	strategy := MostFreeSpaceStrategy{Nodes: nodes}

	selected, err := strategy.SelectNodes("chunk1", 5)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not enough nodes")
	require.Len(t, selected, 2)
}

func TestMostFreeSpaceStrategy_SelectNodes_WithExclude(t *testing.T) {
	nodes := []fsm.NodeEntry{
		{NodeID: "n1", FreeSpace: 100},
		{NodeID: "n2", FreeSpace: 500},
		{NodeID: "n3", FreeSpace: 300},
	}

	strategy := MostFreeSpaceStrategy{Nodes: nodes}

	selected, err := strategy.SelectNodes("chunk1", 2, fsm.NodeEntry{NodeID: "n2"})
	require.NoError(t, err)
	require.Len(t, selected, 2)
	require.Equal(t, "n3", selected[0].NodeID)
	require.Equal(t, "n1", selected[1].NodeID)
}

func TestMostFreeSpaceStrategy_SelectNodes_ExcludeAll(t *testing.T) {
	nodes := []fsm.NodeEntry{
		{NodeID: "n1", FreeSpace: 100},
		{NodeID: "n2", FreeSpace: 500},
	}

	strategy := MostFreeSpaceStrategy{Nodes: nodes}

	selected, err := strategy.SelectNodes("chunk1", 1,
		fsm.NodeEntry{NodeID: "n1"},
		fsm.NodeEntry{NodeID: "n2"},
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not enough nodes")
	require.Len(t, selected, 0)
}

func TestMostFreeSpaceStrategy_SelectNodeReverse_Success(t *testing.T) {
	nodes := []fsm.NodeEntry{
		{NodeID: "n1", FreeSpace: 100},
		{NodeID: "n2", FreeSpace: 500},
		{NodeID: "n3", FreeSpace: 300},
	}

	strategy := MostFreeSpaceStrategy{Nodes: nodes}

	selected, err := strategy.SelectNodeReverse("chunk1", 2)
	require.NoError(t, err)
	require.Len(t, selected, 2)
	require.Equal(t, "n1", selected[0].NodeID)
	require.Equal(t, "n3", selected[1].NodeID)
}

func TestMostFreeSpaceStrategy_SelectNodeReverse_InsufficientNodes(t *testing.T) {
	nodes := []fsm.NodeEntry{
		{NodeID: "n1", FreeSpace: 100},
	}

	strategy := MostFreeSpaceStrategy{Nodes: nodes}

	selected, err := strategy.SelectNodeReverse("chunk1", 3)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not enough nodes")
	require.Len(t, selected, 1)
}

func TestMostFreeSpaceStrategy_SelectNodeReverse_WithExclude(t *testing.T) {
	nodes := []fsm.NodeEntry{
		{NodeID: "n1", FreeSpace: 100},
		{NodeID: "n2", FreeSpace: 500},
		{NodeID: "n3", FreeSpace: 300},
	}

	strategy := MostFreeSpaceStrategy{Nodes: nodes}

	selected, err := strategy.SelectNodeReverse("chunk1", 1, fsm.NodeEntry{NodeID: "n1"})
	require.NoError(t, err)
	require.Len(t, selected, 1)
	require.Equal(t, "n3", selected[0].NodeID)
}

func TestMostFreeSpaceStrategy_SelectPrimary_Success(t *testing.T) {
	nodes := []fsm.NodeEntry{
		{NodeID: "n1", FreeSpace: 100},
		{NodeID: "n2", FreeSpace: 500},
		{NodeID: "n3", FreeSpace: 300},
	}

	strategy := MostFreeSpaceStrategy{Nodes: nodes}

	primary := strategy.SelectPrimary(nodes)
	require.Equal(t, "n2", primary.NodeID)
	require.Equal(t, uint64(500), primary.FreeSpace)
}

func TestMostFreeSpaceStrategy_SelectPrimary_EmptyNodes(t *testing.T) {
	strategy := MostFreeSpaceStrategy{Nodes: []fsm.NodeEntry{}}

	primary := strategy.SelectPrimary(nil)
	require.Empty(t, primary.NodeID)
}

func TestFilterNodes(t *testing.T) {
	nodes := []fsm.NodeEntry{
		{NodeID: "n1", FreeSpace: 100},
		{NodeID: "n2", FreeSpace: 200},
		{NodeID: "n3", FreeSpace: 300},
	}

	filtered := filterNodes(nodes, fsm.NodeEntry{NodeID: "n2"})

	require.Len(t, filtered, 2)
	require.Equal(t, "n1", filtered[0].NodeID)
	require.Equal(t, "n3", filtered[1].NodeID)
}

func TestFilterNodes_NoExclusions(t *testing.T) {
	nodes := []fsm.NodeEntry{
		{NodeID: "n1", FreeSpace: 100},
		{NodeID: "n2", FreeSpace: 200},
	}

	filtered := filterNodes(nodes)

	require.Len(t, filtered, 2)
}

func TestFilterNodes_ExcludeAll(t *testing.T) {
	nodes := []fsm.NodeEntry{
		{NodeID: "n1", FreeSpace: 100},
		{NodeID: "n2", FreeSpace: 200},
	}

	filtered := filterNodes(nodes,
		fsm.NodeEntry{NodeID: "n1"},
		fsm.NodeEntry{NodeID: "n2"},
	)

	require.Len(t, filtered, 0)
}

func TestMostFreeSpaceStrategy_SelectNodes_CountEqualToAvailable(t *testing.T) {
	nodes := []fsm.NodeEntry{
		{NodeID: "n1", FreeSpace: 100},
		{NodeID: "n2", FreeSpace: 500},
	}

	strategy := MostFreeSpaceStrategy{Nodes: nodes}

	selected, err := strategy.SelectNodes("chunk1", 2)
	require.NoError(t, err)
	require.Len(t, selected, 2)
}
