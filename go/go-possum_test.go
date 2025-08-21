package possum

import (
	"testing"

	_ "github.com/anacrolix/possum/go/testlink"
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
