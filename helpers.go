package restic

import "github.com/restic/restic/backend"

// Collect collects all IDs received from the channel and returns a slice which
// contains all IDs in the order the have been received.
func Collect(ch <-chan backend.ID) (list backend.IDs) {
	for id := range ch {
		list = append(list, id)
	}

	return list
}
