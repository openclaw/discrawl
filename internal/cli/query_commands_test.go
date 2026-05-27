package cli

import (
	"reflect"
	"testing"
)

func TestPermuteSearchFlags(t *testing.T) {
	got := permuteSearchFlags([]string{
		"worker",
		"--channel", "c1",
		"--include-empty",
		"--mode=fts",
		"--guilds=g1",
		"--",
		"tail",
	})
	want := []string{"--channel", "c1", "--include-empty", "--mode=fts", "--guilds=g1", "worker", "tail"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("permuted = %#v", got)
	}
}
