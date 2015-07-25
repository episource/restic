package checker

import (
	"github.com/restic/restic/backend"
	"github.com/restic/restic/repository"
)

// Repacker extracts still used blobs from packs with unused blobs and creates
// new packs.
type Repacker struct {
	UnusedBlobs []backend.ID
}

// Repack runs the process of finding still used blobs in packs with unused
// blobs, extracts them and creates new packs with just the still-in-use blobs.
func (r *Repacker) Repack() error {

	return nil
}

// uniqIDs returns list without any duplicate IDs.
func uniqIDs(list []backend.ID) []backend.ID {
	known := make(map[mapID]struct{})
	for pos := 0; pos < len(list); pos++ {
		id := list[pos]
		if _, ok := known[id2map(id)]; ok {
			list = append(list[:pos], list[pos+1:]...)
			continue
		}

		known[id2map(id)] = struct{}{}
	}

	return list
}

// FindPacksforBlobs returns the list of packs that contain the blobs.
func FindPacksforBlobs(repo *repository.Repository, blobs []backend.ID) ([]backend.ID, error) {
	var packs []backend.ID
	idx := repo.Index()
	for _, id := range blobs {
		packID, _, _, _, err := idx.Lookup(id)
		if err != nil {
			return nil, err
		}

		packs = append(packs, packID)
	}

	return uniqIDs(packs), nil
}
