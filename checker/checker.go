package checker

import (
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/restic/restic"
	"github.com/restic/restic/backend"
	"github.com/restic/restic/debug"
	"github.com/restic/restic/repository"
)

// Checker runs various checks on a repository. It is advisable to create an
// exclusive Lock in the repository before running any checks.
//
// A Checker only tests for internal errors within the data structures of the
// repository (e.g. missing blobs), and needs a valid Repository to work on.
type Checker struct {
	packs    map[backend.ID]struct{}
	blobs    map[backend.ID]struct{}
	blobRefs struct {
		sync.Mutex
		M map[backend.ID]uint
	}
	indexes       map[backend.ID]*repository.Index
	orphanedPacks backend.IDs

	masterIndex *repository.Index

	repo *repository.Repository
}

// New returns a new checker which runs on repo.
func New(repo *repository.Repository) *Checker {
	c := &Checker{
		packs:       make(map[backend.ID]struct{}),
		blobs:       make(map[backend.ID]struct{}),
		masterIndex: repository.NewIndex(),
		indexes:     make(map[backend.ID]*repository.Index),
		repo:        repo,
	}

	c.blobRefs.M = make(map[backend.ID]uint)

	return c
}

const defaultParallelism = 40

// LoadIndex loads all index files.
func (c *Checker) LoadIndex() error {
	debug.Log("LoadIndex", "Start")
	type indexRes struct {
		Index *repository.Index
		ID    string
	}

	indexCh := make(chan indexRes)

	worker := func(id backend.ID, done <-chan struct{}) error {
		debug.Log("LoadIndex", "worker got index %v", id)
		idx, err := repository.LoadIndexWithDecoder(c.repo, id.String(), repository.DecodeIndex)
		if err == repository.ErrOldIndexFormat {
			debug.Log("LoadIndex", "old index format found, converting")
			fmt.Fprintf(os.Stderr, "convert index %v to new format\n", id.Str())
			id, err = repository.ConvertIndex(c.repo, id)
			if err != nil {
				return err
			}

			idx, err = repository.LoadIndexWithDecoder(c.repo, id.String(), repository.DecodeIndex)
		}

		if err != nil {
			return err
		}

		select {
		case indexCh <- indexRes{Index: idx, ID: id.String()}:
		case <-done:
		}

		return nil
	}

	var perr error
	go func() {
		defer close(indexCh)
		debug.Log("LoadIndex", "start loading indexes in parallel")
		perr = repository.FilesInParallel(c.repo.Backend(), backend.Index, defaultParallelism,
			repository.ParallelWorkFuncParseID(worker))
		debug.Log("LoadIndex", "loading indexes finished, error: %v", perr)
	}()

	done := make(chan struct{})
	defer close(done)

	for res := range indexCh {
		debug.Log("LoadIndex", "process index %v", res.ID)
		id, err := backend.ParseID(res.ID)
		if err != nil {
			return err
		}

		c.indexes[id] = res.Index
		c.masterIndex.Merge(res.Index)

		debug.Log("LoadIndex", "process blobs")
		cnt := 0
		for blob := range res.Index.Each(done) {
			c.packs[blob.PackID] = struct{}{}
			c.blobs[blob.ID] = struct{}{}
			c.blobRefs.M[blob.ID] = 0
			cnt++
		}

		debug.Log("LoadIndex", "%d blobs processed", cnt)
	}

	debug.Log("LoadIndex", "done, error %v", perr)

	c.repo.SetIndex(c.masterIndex)

	return perr
}

// PackError describes an error with a specific pack.
type PackError struct {
	ID       backend.ID
	Orphaned bool
	Err      error
}

func (e PackError) Error() string {
	return "pack " + e.ID.String() + ": " + e.Err.Error()
}

func packIDTester(repo *repository.Repository, inChan <-chan backend.ID, errChan chan<- error, wg *sync.WaitGroup, done <-chan struct{}) {
	debug.Log("Checker.testPackID", "worker start")
	defer debug.Log("Checker.testPackID", "worker done")

	defer wg.Done()

	for id := range inChan {
		ok, err := repo.Backend().Test(backend.Data, id.String())
		if err != nil {
			err = PackError{ID: id, Err: err}
		} else {
			if !ok {
				err = PackError{ID: id, Err: errors.New("does not exist")}
			}
		}

		if err != nil {
			debug.Log("Checker.testPackID", "error checking for pack %s: %v", id.Str(), err)
			select {
			case <-done:
				return
			case errChan <- err:
			}

			continue
		}

		debug.Log("Checker.testPackID", "pack %s exists", id.Str())
	}
}

// Packs checks that all packs referenced in the index are still available and
// there are no packs that aren't in an index. errChan is closed after all
// packs have been checked.
func (c *Checker) Packs(errChan chan<- error, done <-chan struct{}) {
	defer close(errChan)

	debug.Log("Checker.Packs", "checking for %d packs", len(c.packs))
	seenPacks := make(map[backend.ID]struct{})

	var workerWG sync.WaitGroup

	IDChan := make(chan backend.ID)
	for i := 0; i < defaultParallelism; i++ {
		workerWG.Add(1)
		go packIDTester(c.repo, IDChan, errChan, &workerWG, done)
	}

	for id := range c.packs {
		seenPacks[id] = struct{}{}
		IDChan <- id
	}
	close(IDChan)

	debug.Log("Checker.Packs", "waiting for %d workers to terminate", defaultParallelism)
	workerWG.Wait()
	debug.Log("Checker.Packs", "workers terminated")

	for id := range c.repo.List(backend.Data, done) {
		debug.Log("Checker.Packs", "check data blob %v", id.Str())
		if _, ok := seenPacks[id]; !ok {
			c.orphanedPacks = append(c.orphanedPacks, id)
			select {
			case <-done:
				return
			case errChan <- PackError{ID: id, Orphaned: true, Err: errors.New("not referenced in any index")}:
			}
		}
	}
}

// Error is an error that occurred while checking a repository.
type Error struct {
	TreeID *backend.ID
	BlobID *backend.ID
	Err    error
}

func (e Error) Error() string {
	if e.BlobID != nil && e.TreeID != nil {
		msg := "tree " + e.TreeID.String()
		msg += ", blob " + e.BlobID.String()
		msg += ": " + e.Err.Error()
		return msg
	}

	if e.TreeID != nil {
		return "tree " + e.TreeID.String() + ": " + e.Err.Error()
	}

	return e.Err.Error()
}

func loadTreeFromSnapshot(repo *repository.Repository, id backend.ID) (backend.ID, error) {
	sn, err := restic.LoadSnapshot(repo, id)
	if err != nil {
		debug.Log("Checker.loadTreeFromSnapshot", "error loading snapshot %v: %v", id.Str(), err)
		return backend.ID{}, err
	}

	if sn.Tree == nil {
		debug.Log("Checker.loadTreeFromSnapshot", "snapshot %v has no tree", id.Str())
		return backend.ID{}, fmt.Errorf("snapshot %v has no tree", id)
	}

	return *sn.Tree, nil
}

// loadSnapshotTreeIDs loads all snapshots from backend and returns the tree IDs.
func loadSnapshotTreeIDs(repo *repository.Repository) (backend.IDs, []error) {
	var trees struct {
		IDs backend.IDs
		sync.Mutex
	}

	var errs struct {
		errs []error
		sync.Mutex
	}

	snapshotWorker := func(strID string, done <-chan struct{}) error {
		id, err := backend.ParseID(strID)
		if err != nil {
			return err
		}

		debug.Log("Checker.Snaphots", "load snapshot %v", id.Str())

		treeID, err := loadTreeFromSnapshot(repo, id)
		if err != nil {
			errs.Lock()
			errs.errs = append(errs.errs, err)
			errs.Unlock()
			return nil
		}

		debug.Log("Checker.Snaphots", "snapshot %v has tree %v", id.Str(), treeID.Str())
		trees.Lock()
		trees.IDs = append(trees.IDs, treeID)
		trees.Unlock()

		return nil
	}

	err := repository.FilesInParallel(repo.Backend(), backend.Snapshot, defaultParallelism, snapshotWorker)
	if err != nil {
		errs.errs = append(errs.errs, err)
	}

	return trees.IDs, errs.errs
}

// TreeError is returned when loading a tree from the repository failed.
type TreeError struct {
	ID     backend.ID
	Errors []error
}

func (e TreeError) Error() string {
	return fmt.Sprintf("%v: %d errors", e.ID.String(), len(e.Errors))
}

type treeJob struct {
	backend.ID
	error
	*restic.Tree
}

// loadTreeWorker loads trees from repo and sends them to out.
func loadTreeWorker(repo *repository.Repository,
	in <-chan backend.ID, out chan<- treeJob,
	done <-chan struct{}, wg *sync.WaitGroup) {

	defer func() {
		debug.Log("checker.loadTreeWorker", "exiting")
		wg.Done()
	}()

	var (
		inCh  = in
		outCh = out
		job   treeJob
	)

	outCh = nil
	for {
		select {
		case <-done:
			return

		case treeID, ok := <-inCh:
			if !ok {
				return
			}
			debug.Log("checker.loadTreeWorker", "load tree %v", treeID.Str())

			tree, err := restic.LoadTree(repo, treeID)
			debug.Log("checker.loadTreeWorker", "load tree %v (%v) returned err %v", tree, treeID.Str(), err)
			job = treeJob{ID: treeID, error: err, Tree: tree}
			outCh = out
			inCh = nil

		case outCh <- job:
			debug.Log("checker.loadTreeWorker", "sent tree %v", job.ID.Str())
			outCh = nil
			inCh = in
		}
	}
}

// checkTreeWorker checks the trees received and sends out errors to errChan.
func (c *Checker) checkTreeWorker(in <-chan treeJob, out chan<- TreeError, done <-chan struct{}, wg *sync.WaitGroup) {
	defer func() {
		debug.Log("checker.checkTreeWorker", "exiting")
		wg.Done()
	}()

	var (
		inCh      = in
		outCh     = out
		treeError TreeError
	)

	outCh = nil
	for {
		select {
		case <-done:
			return

		case job, ok := <-inCh:
			if !ok {
				return
			}

			id := job.ID
			alreadyChecked := false
			c.blobRefs.Lock()
			if c.blobRefs.M[id] > 0 {
				alreadyChecked = true
			}
			c.blobRefs.M[id]++
			debug.Log("checker.checkTreeWorker", "tree %v refcount %d", job.ID.Str(), c.blobRefs.M[id])
			c.blobRefs.Unlock()

			if alreadyChecked {
				continue
			}

			debug.Log("checker.checkTreeWorker", "load tree %v", job.ID.Str())

			errs := c.checkTree(job.ID, job.Tree)
			if len(errs) > 0 {
				debug.Log("checker.checkTreeWorker", "checked tree %v: %v errors", job.ID.Str(), len(errs))
				treeError = TreeError{ID: job.ID, Errors: errs}
				outCh = out
				inCh = nil
			}

		case outCh <- treeError:
			debug.Log("checker.checkTreeWorker", "tree %v: sent %d errors", treeError.ID, len(treeError.Errors))
			outCh = nil
			inCh = in
		}
	}
}

func filterTrees(backlog backend.IDs, loaderChan chan<- backend.ID, in <-chan treeJob, out chan<- treeJob, done <-chan struct{}) {
	defer func() {
		debug.Log("checker.filterTrees", "closing output channels")
		close(loaderChan)
		close(out)
	}()

	var (
		inCh                    = in
		outCh                   = out
		loadCh                  = loaderChan
		job                     treeJob
		nextTreeID              backend.ID
		outstandingLoadTreeJobs = 0
	)

	outCh = nil
	loadCh = nil

	for {
		if loadCh == nil && len(backlog) > 0 {
			loadCh = loaderChan
			nextTreeID, backlog = backlog[0], backlog[1:]
		}

		if loadCh == nil && outCh == nil && outstandingLoadTreeJobs == 0 {
			debug.Log("checker.filterTrees", "backlog is empty, all channels nil, exiting")
			return
		}

		select {
		case <-done:
			return

		case loadCh <- nextTreeID:
			outstandingLoadTreeJobs++
			loadCh = nil

		case j, ok := <-inCh:
			if !ok {
				debug.Log("checker.filterTrees", "input channel closed")
				inCh = nil
				in = nil
				continue
			}

			outstandingLoadTreeJobs--
			debug.Log("checker.filterTrees", "input job tree %v", j.ID.Str())

			backlog = append(backlog, j.Tree.Subtrees()...)

			job = j
			outCh = out
			inCh = nil

		case outCh <- job:
			outCh = nil
			inCh = in
		}
	}
}

// Structure checks that for all snapshots all referenced data blobs and
// subtrees are available in the index. errChan is closed after all trees have
// been traversed.
func (c *Checker) Structure(errChan chan<- error, done <-chan struct{}) {
	defer close(errChan)

	trees, errs := loadSnapshotTreeIDs(c.repo)
	debug.Log("checker.Structure", "need to check %d trees from snapshots, %d errs returned", len(trees), len(errs))

	for _, err := range errs {
		select {
		case <-done:
			return
		case errChan <- err:
		}
	}

	treeIDChan := make(chan backend.ID)
	treeJobChan1 := make(chan treeJob)
	treeJobChan2 := make(chan treeJob)
	treeErrChan := make(chan TreeError)

	var wg sync.WaitGroup
	for i := 0; i < defaultParallelism; i++ {
		wg.Add(2)
		go loadTreeWorker(c.repo, treeIDChan, treeJobChan1, done, &wg)
		go c.checkTreeWorker(treeJobChan2, treeErrChan, done, &wg)
	}

	filterTrees(trees, treeIDChan, treeJobChan1, treeJobChan2, done)

	wg.Wait()
}

func (c *Checker) checkTree(id backend.ID, tree *restic.Tree) (errs []error) {
	debug.Log("Checker.checkTree", "checking tree %v", id.Str())

	var blobs []backend.ID

	for i, node := range tree.Nodes {
		switch node.Type {
		case "file":
			blobs = append(blobs, node.Content...)
		case "dir":
			if node.Subtree == nil {
				errs = append(errs, Error{TreeID: &id, Err: fmt.Errorf("node %d is dir but has no subtree", i)})
				continue
			}
		}
	}

	for _, blobID := range blobs {
		c.blobRefs.Lock()
		c.blobRefs.M[blobID]++
		debug.Log("Checker.checkTree", "blob %v refcount %d", blobID.Str(), c.blobRefs.M[blobID])
		c.blobRefs.Unlock()

		if _, ok := c.blobs[blobID]; !ok {
			debug.Log("Checker.trees", "tree %v references blob %v which isn't contained in index", id.Str(), blobID.Str())

			errs = append(errs, Error{TreeID: &id, BlobID: &blobID, Err: errors.New("not found in index")})
		}
	}

	return errs
}

// UnusedBlobs returns all blobs that have never been referenced.
func (c *Checker) UnusedBlobs() (blobs backend.IDs) {
	c.blobRefs.Lock()
	defer c.blobRefs.Unlock()

	debug.Log("Checker.UnusedBlobs", "checking %d blobs", len(c.blobs))
	for id := range c.blobs {
		if c.blobRefs.M[id] == 0 {
			debug.Log("Checker.UnusedBlobs", "blob %v not not referenced", id.Str())
			blobs = append(blobs, id)
		}
	}

	return blobs
}

// OrphanedPacks returns a slice of unused packs (only available after Packs() was run).
func (c *Checker) OrphanedPacks() backend.IDs {
	return c.orphanedPacks
}
