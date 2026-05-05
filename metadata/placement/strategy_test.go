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

	strategy := MostFreeSpaceStrategy{}

	selected, err := strategy.SelectNodes(nodes, "chunk1", 0, 2)
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

	strategy := MostFreeSpaceStrategy{}

	selected, err := strategy.SelectNodes(nodes, "chunk1", 0, 5)
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

	strategy := MostFreeSpaceStrategy{}

	selected, err := strategy.SelectNodes(nodes, "chunk1", 0, 2, fsm.NodeEntry{NodeID: "n2"})
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

	strategy := MostFreeSpaceStrategy{}

	selected, err := strategy.SelectNodes(nodes, "chunk1", 0, 1,
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

	strategy := MostFreeSpaceStrategy{}

	selected, err := strategy.SelectNodeReverse(nodes, "chunk1", 0, 2)
	require.NoError(t, err)
	require.Len(t, selected, 2)
	require.Equal(t, "n1", selected[0].NodeID)
	require.Equal(t, "n3", selected[1].NodeID)
}

func TestMostFreeSpaceStrategy_SelectNodeReverse_InsufficientNodes(t *testing.T) {
	nodes := []fsm.NodeEntry{
		{NodeID: "n1", FreeSpace: 100},
	}

	strategy := MostFreeSpaceStrategy{}

	selected, err := strategy.SelectNodeReverse(nodes, "chunk1", 0, 3)
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

	strategy := MostFreeSpaceStrategy{}

	selected, err := strategy.SelectNodeReverse(nodes, "chunk1", 0, 1, fsm.NodeEntry{NodeID: "n1"})
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

	strategy := MostFreeSpaceStrategy{}

	primary := strategy.SelectPrimary(nodes)
	require.Equal(t, "n2", primary.NodeID)
	require.Equal(t, uint64(500), primary.FreeSpace)
}

func TestMostFreeSpaceStrategy_SelectPrimary_EmptyNodes(t *testing.T) {
	strategy := MostFreeSpaceStrategy{}

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

	strategy := MostFreeSpaceStrategy{}

	selected, err := strategy.SelectNodes(nodes, "chunk1", 0, 2)
	require.NoError(t, err)
	require.Len(t, selected, 2)
}

// ---------------------------------------------------------------------------
// minSpace filtering tests
// ---------------------------------------------------------------------------

func TestFilterByMinSpace(t *testing.T) {
	nodes := []fsm.NodeEntry{
		{NodeID: "n1", FreeSpace: 100},
		{NodeID: "n2", FreeSpace: 50},
		{NodeID: "n3", FreeSpace: 0},
	}

	filtered := filterByMinSpace(nodes, 50)
	require.Len(t, filtered, 2)
	require.Equal(t, "n1", filtered[0].NodeID)
	require.Equal(t, "n2", filtered[1].NodeID)
}

func TestFilterByMinSpace_ZeroMinMeansNoFilter(t *testing.T) {
	nodes := []fsm.NodeEntry{
		{NodeID: "n1", FreeSpace: 100},
		{NodeID: "n2", FreeSpace: 0},
	}

	filtered := filterByMinSpace(nodes, 0)
	require.Len(t, filtered, 2)
}

func TestFilterByMinSpace_AllFiltered(t *testing.T) {
	nodes := []fsm.NodeEntry{
		{NodeID: "n1", FreeSpace: 10},
		{NodeID: "n2", FreeSpace: 5},
	}

	filtered := filterByMinSpace(nodes, 100)
	require.Len(t, filtered, 0)
}

func TestMostFreeSpaceStrategy_SelectNodes_ExcludesFullNode(t *testing.T) {
	nodes := []fsm.NodeEntry{
		{NodeID: "n1", FreeSpace: 1000},
		{NodeID: "n2", FreeSpace: 0},
		{NodeID: "n3", FreeSpace: 100},
	}

	strategy := MostFreeSpaceStrategy{}

	selected, err := strategy.SelectNodes(nodes, "chunk1", 500, 1)
	require.NoError(t, err)
	require.Len(t, selected, 1)
	require.Equal(t, "n1", selected[0].NodeID)
}

func TestMostFreeSpaceStrategy_SelectNodes_AllFull(t *testing.T) {
	nodes := []fsm.NodeEntry{
		{NodeID: "n1", FreeSpace: 10},
		{NodeID: "n2", FreeSpace: 5},
	}

	strategy := MostFreeSpaceStrategy{}

	selected, err := strategy.SelectNodes(nodes, "chunk1", 500, 1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not enough nodes")
	require.Len(t, selected, 0)
}

func TestMostFreeSpaceStrategy_SelectNodeReverse_ExcludesFullNode(t *testing.T) {
	nodes := []fsm.NodeEntry{
		{NodeID: "n1", FreeSpace: 1000},
		{NodeID: "n2", FreeSpace: 0},
		{NodeID: "n3", FreeSpace: 100},
	}

	strategy := MostFreeSpaceStrategy{}

	selected, err := strategy.SelectNodeReverse(nodes, "chunk1", 500, 1)
	require.NoError(t, err)
	require.Len(t, selected, 1)
	require.Equal(t, "n1", selected[0].NodeID)
}

func TestMinSpaceZeroDoesNotFilter(t *testing.T) {
	nodes := []fsm.NodeEntry{
		{NodeID: "n1", FreeSpace: 1000},
		{NodeID: "n2", FreeSpace: 0},
	}

	strategy := MostFreeSpaceStrategy{}
	selected, err := strategy.SelectNodes(nodes, "chunk1", 0, 2)
	require.NoError(t, err)
	require.Len(t, selected, 2)
}

// ---------------------------------------------------------------------------
// LeastLoadedStrategy tests
// ---------------------------------------------------------------------------

func TestLeastLoadedStrategy_SelectPrimary(t *testing.T) {
	nodes := []fsm.NodeEntry{
		{NodeID: "heavy", ChunkCount: 100},
		{NodeID: "light", ChunkCount: 1},
		{NodeID: "medium", ChunkCount: 50},
	}

	strategy := LeastLoadedStrategy{}
	primary := strategy.SelectPrimary(nodes)

	require.Equal(t, "light", primary.NodeID)
}

func TestLeastLoadedStrategy_SelectPrimary_Empty(t *testing.T) {
	strategy := LeastLoadedStrategy{}
	primary := strategy.SelectPrimary(nil)

	require.Empty(t, primary.NodeID)
}

func TestLeastLoadedStrategy_SelectNodes(t *testing.T) {
	nodes := []fsm.NodeEntry{
		{NodeID: "heavy", ChunkCount: 100},
		{NodeID: "light", ChunkCount: 1},
		{NodeID: "medium", ChunkCount: 50},
	}

	strategy := LeastLoadedStrategy{}
	selected, err := strategy.SelectNodes(nodes, "chunk1", 0, 2)

	require.NoError(t, err)
	require.Len(t, selected, 2)
	require.Equal(t, "light", selected[0].NodeID)
	require.Equal(t, "medium", selected[1].NodeID)
}

func TestLeastLoadedStrategy_SelectNodes_MinSpace(t *testing.T) {
	nodes := []fsm.NodeEntry{
		{NodeID: "big", ChunkCount: 1, FreeSpace: 1000},
		{NodeID: "small", ChunkCount: 2, FreeSpace: 10},
	}

	strategy := LeastLoadedStrategy{}
	selected, err := strategy.SelectNodes(nodes, "chunk1", 500, 1)

	require.NoError(t, err)
	require.Len(t, selected, 1)
	require.Equal(t, "big", selected[0].NodeID)
}

func TestLeastLoadedStrategy_SelectNodes_WithExclude(t *testing.T) {
	nodes := []fsm.NodeEntry{
		{NodeID: "heavy", ChunkCount: 100},
		{NodeID: "light", ChunkCount: 1},
		{NodeID: "medium", ChunkCount: 50},
	}

	strategy := LeastLoadedStrategy{}
	selected, err := strategy.SelectNodes(nodes, "chunk1", 0, 1, fsm.NodeEntry{NodeID: "light"})

	require.NoError(t, err)
	require.Len(t, selected, 1)
	require.Equal(t, "medium", selected[0].NodeID)
}

func TestLeastLoadedStrategy_SelectNodeReverse(t *testing.T) {
	nodes := []fsm.NodeEntry{
		{NodeID: "heavy", ChunkCount: 100},
		{NodeID: "light", ChunkCount: 1},
		{NodeID: "medium", ChunkCount: 50},
	}

	strategy := LeastLoadedStrategy{}
	selected, err := strategy.SelectNodeReverse(nodes, "chunk1", 0, 2)

	require.NoError(t, err)
	require.Len(t, selected, 2)
	require.Equal(t, "heavy", selected[0].NodeID)
	require.Equal(t, "medium", selected[1].NodeID)
}

func TestLeastLoadedStrategy_SelectNodes_Insufficient(t *testing.T) {
	nodes := []fsm.NodeEntry{
		{NodeID: "n1", ChunkCount: 5},
	}

	strategy := LeastLoadedStrategy{}
	selected, err := strategy.SelectNodes(nodes, "chunk1", 0, 3)

	require.Error(t, err)
	require.Contains(t, err.Error(), "not enough nodes")
	require.Len(t, selected, 1)
}
