package checker_test

import (
	"testing"

	"github.com/restic/restic/backend"
	"github.com/restic/restic/checker"

	. "github.com/restic/restic/test"
)

var findPackTests = []struct {
	blobIDs, packIDs []backend.ID
}{
	{
		[]backend.ID{
			ParseID("534f211b4fc2cf5b362a24e8eba22db5372a75b7e974603ff9263f5a471760f4"),
			ParseID("51aa04744b518c6a85b4e7643cfa99d58789c2a6ca2a3fda831fa3032f28535c"),
			ParseID("454515bca5f4f60349a527bd814cc2681bc3625716460cc6310771c966d8a3bf"),
			ParseID("c01952de4d91da1b1b80bc6e06eaa4ec21523f4853b69dc8231708b9b7ec62d8"),
		},
		[]backend.ID{
			ParseID("19a731a515618ec8b75fc0ff3b887d8feb83aef1001c9899f6702761142ed068"),
			ParseID("657f7fb64f6a854fff6fe9279998ee09034901eded4e6db9bcee0e59745bbce6"),
		},
	},
}

func TestRepackerFindPacks(t *testing.T) {
	WithTestEnvironment(t, checkerTestData, func(repodir string) {
		repo := OpenLocalRepo(t, repodir)

		OK(t, repo.LoadIndex())

		for _, test := range findPackTests {
			packIDs, err := checker.FindPacksforBlobs(repo, test.blobIDs)
			OK(t, err)
			Equals(t, test.packIDs, packIDs)
		}
	})
}
