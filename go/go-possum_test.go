package possum

import (
	"testing"
)

func TestOpenClose(t *testing.T) {
	handle, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	err = handle.Close()
	if err != nil {
		t.Fatal(err)
	}
}
