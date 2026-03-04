package utils

import "testing"

func TestIsValidPath(t *testing.T) {
	cases := []struct {
		name string
		path string
		want bool
	}{
		{"absolute path", "/home/user/data", true},
		{"relative path", "data/chunks", true},
		{"dot path", ".", true},
		{"empty string", "", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := IsValidPath(c.path); got != c.want {
				t.Errorf("IsValidPath(%q) = %v, want %v", c.path, got, c.want)
			}
		})
	}
}
