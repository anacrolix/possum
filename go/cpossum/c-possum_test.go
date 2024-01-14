package possumC

import (
	qt "github.com/frankban/quicktest"
	"testing"
)

func TestNoSuchKey(t *testing.T) {
	c := qt.New(t)
	c.Check(NoSuchKey, qt.ErrorIs, NoSuchKey)
	c.Check(Error{
		pec:             cErrorEnumNoSuchKey,
		displayGoesHere: "some string not in the NoSuchKey global target",
	}, qt.ErrorIs, NoSuchKey)
}
