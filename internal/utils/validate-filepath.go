package utils

import "path/filepath"

func IsValidPath(path string) bool {
	return filepath.IsAbs(path) || path != ""
}
