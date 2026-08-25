package tlds

import "testing"

func TestIsICANNTld(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{"com", true},
		{"COM", true},
		{"net", true},
		{"org", true},
		{"site", true},
		{"io", true},
		{"dev", true},
		{"app", true},
		{"altroot", false},
		{"", false},
		{"a b", false},
	}
	for _, c := range cases {
		if got := IsICANNTld(c.in); got != c.want {
			t.Errorf("IsICANNTld(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

func TestIsICANN(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{"example.com", true},
		{"www.example.com", true},
		{"starter.pinned.site", true},
		{"pinned.site", true},
		{"lumeweb", false},
		{"blog.altroot", false},
		{"", false},
		{"example.", false},
	}
	for _, c := range cases {
		if got := IsICANN(c.in); got != c.want {
			t.Errorf("IsICANN(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}
