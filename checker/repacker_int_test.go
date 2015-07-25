package checker

import (
	"reflect"
	"testing"

	"github.com/restic/restic/backend"
	. "github.com/restic/restic/test"
)

var uniqTests = []struct {
	before, after []backend.ID
}{
	{
		[]backend.ID{
			ParseID("7bb086db0d06285d831485da8031281e28336a56baa313539eaea1c73a2a1a40"),
			ParseID("1285b30394f3b74693cc29a758d9624996ae643157776fce8154aabd2f01515f"),
			ParseID("7bb086db0d06285d831485da8031281e28336a56baa313539eaea1c73a2a1a40"),
		},
		[]backend.ID{
			ParseID("7bb086db0d06285d831485da8031281e28336a56baa313539eaea1c73a2a1a40"),
			ParseID("1285b30394f3b74693cc29a758d9624996ae643157776fce8154aabd2f01515f"),
		},
	},
	{
		[]backend.ID{
			ParseID("1285b30394f3b74693cc29a758d9624996ae643157776fce8154aabd2f01515f"),
			ParseID("7bb086db0d06285d831485da8031281e28336a56baa313539eaea1c73a2a1a40"),
			ParseID("7bb086db0d06285d831485da8031281e28336a56baa313539eaea1c73a2a1a40"),
		},
		[]backend.ID{
			ParseID("1285b30394f3b74693cc29a758d9624996ae643157776fce8154aabd2f01515f"),
			ParseID("7bb086db0d06285d831485da8031281e28336a56baa313539eaea1c73a2a1a40"),
		},
	},
	{
		[]backend.ID{
			ParseID("1285b30394f3b74693cc29a758d9624996ae643157776fce8154aabd2f01515f"),
			ParseID("f658198b405d7e80db5ace1980d125c8da62f636b586c46bf81dfb856a49d0c8"),
			ParseID("7bb086db0d06285d831485da8031281e28336a56baa313539eaea1c73a2a1a40"),
			ParseID("7bb086db0d06285d831485da8031281e28336a56baa313539eaea1c73a2a1a40"),
		},
		[]backend.ID{
			ParseID("1285b30394f3b74693cc29a758d9624996ae643157776fce8154aabd2f01515f"),
			ParseID("f658198b405d7e80db5ace1980d125c8da62f636b586c46bf81dfb856a49d0c8"),
			ParseID("7bb086db0d06285d831485da8031281e28336a56baa313539eaea1c73a2a1a40"),
		},
	},
}

func TestUniqIDs(t *testing.T) {
	for i, test := range uniqTests {
		if !reflect.DeepEqual(uniqIDs(test.before), test.after) {
			t.Errorf("uniqIDs() test %v failed", i)
		}
	}
}
