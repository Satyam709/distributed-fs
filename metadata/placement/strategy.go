package placement

import (
	"fmt"
	"sort"

	"github.com/satyam709/distributed-fs/metadata/fsm"
)

type PlacementStrategy interface {
	SelectNodes(chunkID string, count int, exclude ...fsm.NodeEntry) ([]fsm.NodeEntry, error)
	SelectNodeReverse(chunkID string, count int, exclude ...fsm.NodeEntry) ([]fsm.NodeEntry, error)
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

type MostFreeSpaceStrategy struct {
	Nodes SortByMostFreeSpace
}

func (mfss MostFreeSpaceStrategy) SelectNodes(chunkID string, count int, exclude ...fsm.NodeEntry) ([]fsm.NodeEntry, error) {
	filteredNodes := filterNodes(mfss.Nodes, exclude...)

	sort.Sort(filteredNodes)

	if len(filteredNodes) < count {
		return filteredNodes, fmt.Errorf("there are not enough nodes: required_nodes= %d, have= %d", count, len(filteredNodes))
	}
	return filteredNodes[:count], nil
}
func (mfss MostFreeSpaceStrategy) SelectNodeReverse(chunkID string, count int, exclude ...fsm.NodeEntry) ([]fsm.NodeEntry, error) {
	filteredNodes := filterNodes(mfss.Nodes, exclude...)

	sort.Sort(sort.Reverse(filteredNodes))

	if len(filteredNodes) < count {
		return filteredNodes, fmt.Errorf("there are not enough nodes: required_nodes= %d, have= %d", count, len(filteredNodes))
	}
	return filteredNodes[:count], nil
}

func (mfss MostFreeSpaceStrategy) SelectPrimary(nodes []fsm.NodeEntry) fsm.NodeEntry {
	nodes, err := mfss.SelectNodes("", 1)
	if err != nil || len(nodes) == 0 {
		return fsm.NodeEntry{}
	}
	return nodes[0]
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
