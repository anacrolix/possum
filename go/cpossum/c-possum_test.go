package possumC

import (
	"testing"

	"github.com/go-quicktest/qt"

	_ "github.com/anacrolix/possum/go/testlink"
)

func TestNoSuchKey(t *testing.T) {
	qt.Check(t, qt.ErrorIs(NoSuchKey, NoSuchKey))
	qt.Check(t, qt.ErrorIs(Error{
		pec:             cErrorEnumNoSuchKey,
		displayGoesHere: "some string not in the NoSuchKey global target",
	}, NoSuchKey))
}
