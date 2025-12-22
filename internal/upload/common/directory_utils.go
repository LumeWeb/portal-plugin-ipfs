package common

import (
	"sort"
	"strings"
)

// SortDirectoriesByDepth sorts directory paths from deepest to shallowest
// This ensures children are processed before their parents
func SortDirectoriesByDepth(dirPaths []string) []string {
	sort.Slice(dirPaths, func(i, j int) bool {
		// Count slashes to determine depth
		depthI := strings.Count(dirPaths[i], "/")
		depthJ := strings.Count(dirPaths[j], "/")

		// Sort by depth in descending order (deepest first)
		if depthI != depthJ {
			return depthI > depthJ
		}

		// If same depth, sort alphabetically
		return dirPaths[i] < dirPaths[j]
	})

	return dirPaths
}