package placement

import (
	"fmt"
	"sort"

	"github.com/satyam709/distributed-fs/metadata/fsm"
)

// PlacementStrategy is a pure selection algorithm. The caller is
// responsible for providing the candidate node list — the strategy
// only decides which nodes to pick from that list.
type PlacementStrategy interface {
	SelectNodes(nodes []fsm.NodeEntry, chunkID string, count int, exclude ...fsm.NodeEntry) ([]fsm.NodeEntry, error)
	SelectNodeReverse(nodes []fsm.NodeEntry, chunkID string, count int, exclude ...fsm.NodeEntry) ([]fsm.NodeEntry, error)
	SelectPrimary(nodes []fsm.NodeEntry) fsm.NodeEntry
}

type SortByMostFreeSpace []fsm.NodeEntry

func (a SortByMostFreeSpace) Len() int      { return len(a) }
func (a SortByMostFreeSpace) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a SortByMostFreeSpace) Less(i, j int) bool {
	if a[i].FreeSpace == a[j].FreeSpace {
		return a[i].ChunkCount < a[j].ChunkCount
	}
	return a[i].FreeSpace > a[j].FreeSpace
}

// MostFreeSpaceStrategy is a stateless strategy that selects nodes with
// the most available disk space. All node data is provided by the caller.
type MostFreeSpaceStrategy struct{}

func (mfss MostFreeSpaceStrategy) SelectNodes(nodes []fsm.NodeEntry, chunkID string, count int, exclude ...fsm.NodeEntry) ([]fsm.NodeEntry, error) {
	filteredNodes := SortByMostFreeSpace(filterNodes(nodes, exclude...))

	sort.Sort(filteredNodes)

	if len(filteredNodes) < count {
		return filteredNodes, fmt.Errorf("there are not enough nodes: required_nodes= %d, have= %d", count, len(filteredNodes))
	}
	return filteredNodes[:count], nil
}

func (mfss MostFreeSpaceStrategy) SelectNodeReverse(nodes []fsm.NodeEntry, chunkID string, count int, exclude ...fsm.NodeEntry) ([]fsm.NodeEntry, error) {
	filteredNodes := SortByMostFreeSpace(filterNodes(nodes, exclude...))

	sort.Sort(sort.Reverse(filteredNodes))

	if len(filteredNodes) < count {
		return filteredNodes, fmt.Errorf("there are not enough nodes: required_nodes= %d, have= %d", count, len(filteredNodes))
	}
	return filteredNodes[:count], nil
}

func (mfss MostFreeSpaceStrategy) SelectPrimary(nodes []fsm.NodeEntry) fsm.NodeEntry {
	if len(nodes) == 0 {
		return fsm.NodeEntry{}
	}
	sorted := SortByMostFreeSpace(append([]fsm.NodeEntry(nil), nodes...))
	sort.Sort(sorted)
	return sorted[0]
}

func filterNodes[T interface{ ~[]fsm.NodeEntry }](Nodes T, exclude ...fsm.NodeEntry) T {
	var filteredNodes T
	skipMap := map[string]bool{}
	for _, nd := range exclude {
		skipMap[nd.NodeID] = true
	}
	for _, v := range Nodes {
		if _, ok := skipMap[v.NodeID]; ok {
			continue
		}
		filteredNodes = append(filteredNodes, v)
	}
	return filteredNodes
}

// LeastLoadedStrategy is a stateless strategy that selects nodes with
// the lowest chunk count (least loaded). Useful for picking a source
// node during repair — the node with fewer chunks is less likely to
// be a performance bottleneck.
type LeastLoadedStrategy struct{}

func (lls LeastLoadedStrategy) SelectNodes(nodes []fsm.NodeEntry, chunkID string, count int, exclude ...fsm.NodeEntry) ([]fsm.NodeEntry, error) {
	filtered := filterNodes(nodes, exclude...)
	sort.Slice(filtered, func(i, j int) bool {
		return filtered[i].ChunkCount < filtered[j].ChunkCount
	})
	if len(filtered) < count {
		return filtered, fmt.Errorf("there are not enough nodes: required_nodes= %d, have= %d", count, len(filtered))
	}
	return filtered[:count], nil
}

func (lls LeastLoadedStrategy) SelectNodeReverse(nodes []fsm.NodeEntry, chunkID string, count int, exclude ...fsm.NodeEntry) ([]fsm.NodeEntry, error) {
	filtered := filterNodes(nodes, exclude...)
	sort.Slice(filtered, func(i, j int) bool {
		return filtered[i].ChunkCount > filtered[j].ChunkCount
	})
	if len(filtered) < count {
		return filtered, fmt.Errorf("there are not enough nodes: required_nodes= %d, have= %d", count, len(filtered))
	}
	return filtered[:count], nil
}

func (lls LeastLoadedStrategy) SelectPrimary(nodes []fsm.NodeEntry) fsm.NodeEntry {
	if len(nodes) == 0 {
		return fsm.NodeEntry{}
	}
	best := nodes[0]
	for _, n := range nodes[1:] {
		if n.ChunkCount < best.ChunkCount {
			best = n
		}
	}
	return best
}
