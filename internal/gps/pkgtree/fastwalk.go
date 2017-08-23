// Copyright 2016 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// A faster implementation of filepath.Walk.
//
// filepath.Walk's design necessarily calls os.Lstat on each file,
// even if the caller needs less info. And goimports only need to know
// the type of each file. The kernel interface provides the type in
// the Readdir call but the standard library ignored it.
// fastwalk_unix.go contains a fork of the syscall routines.
//
// See golang.org/issue/16399

package pkgtree

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"sync"
)

// traverseLink is a sentinel error for fastWalk, similar to filepath.SkipDir.
var traverseLink = errors.New("traverse symlink, assuming target is a directory")

// fastWalk walks the file tree rooted at root, calling walkFn for
// each file or directory in the tree, including root.
//
// If fastWalk returns filepath.SkipDir, the directory is skipped.
//
// Unlike filepath.Walk:
//   * file stat calls must be done by the user.
//     The only provided metadata is the file type, which does not include
//     any permission bits.
//   * multiple goroutines stat the filesystem concurrently. The provided
//     walkFn must be safe for concurrent use.
//   * fastWalk can follow symlinks if walkFn returns the traverseLink
//     sentinel error. It is the walkFn's responsibility to prevent
//     fastWalk from going into symlink cycles.
func fastWalk(root string, walkFn func(path string, typ os.FileMode, err error) error) error {
	// TODO(bradfitz): make numWorkers configurable? We used a
	// minimum of 4 to give the kernel more info about multiple
	// things we want, in hopes its I/O scheduling can take
	// advantage of that. Hopefully most are in cache. Maybe 4 is
	// even too low of a minimum. Profile more.
	numWorkers := 4
	if n := runtime.NumCPU(); n > numWorkers {
		numWorkers = n
	}

	// Make sure to wait for all workers to finish, otherwise
	// walkFn could still be called after returning. This Wait call
	// runs after close(e.donec) below.
	var wg sync.WaitGroup
	defer wg.Wait()

	w := &walker{
		fn:       walkFn,
		enqueuec: make(chan string, numWorkers), // buffered for performance
		workc:    make(chan string, numWorkers), // buffered for performance
		donec:    make(chan struct{}),

		// buffered for correctness & not leaking goroutines:
		resc: make(chan error, numWorkers),
	}
	defer close(w.donec)

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go w.doWork(&wg)
	}

	if err := w.fn(root, os.ModeDir, nil); err != nil {
		if err == filepath.SkipDir {
			return nil
		}
		return err
	}

	todo := []string{root}
	out := 0
	for {
		workc := w.workc
		var path string
		if len(todo) == 0 {
			workc = nil
		} else {
			path = todo[len(todo)-1]
		}
		select {
		case workc <- path:
			todo = todo[:len(todo)-1]
			out++
		case it := <-w.enqueuec:
			todo = append(todo, it)
		case err := <-w.resc:
			out--
			if err != nil {
				return err
			}
			if out == 0 && len(todo) == 0 {
				// It's safe to quit here, as long as the buffered
				// enqueue channel isn't also readable, which might
				// happen if the worker sends both another unit of
				// work and its result before the other select was
				// scheduled and both w.resc and w.enqueuec were
				// readable.
				select {
				case it := <-w.enqueuec:
					todo = append(todo, it)
				default:
					return nil
				}
			}
		}
	}
}

// doWork reads directories as instructed (via workc) and runs the
// user's callback function.
func (w *walker) doWork(wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-w.donec:
			return
		case path := <-w.workc:
			select {
			case <-w.donec:
				return
			case w.resc <- w.walk(path):
			}
		}
	}
}

type walker struct {
	fn func(path string, typ os.FileMode, err error) error

	donec    chan struct{} // closed on fastWalk's return
	workc    chan string   // to workers
	enqueuec chan string   // from workers
	resc     chan error    // from workers
}

func (w *walker) enqueue(path string) {
	select {
	case w.enqueuec <- path:
	case <-w.donec:
	}
}

func (w *walker) onDirEnt(dirName, baseName string, typ os.FileMode) error {
	joined := dirName + string(os.PathSeparator) + baseName
	err := w.fn(joined, typ, nil)
	if err == filepath.SkipDir {
		return nil
	} else if err == traverseLink && typ == os.ModeSymlink {
		w.enqueue(joined)
	} else if err != nil {
		return err
	} else if typ == os.ModeDir {
		w.enqueue(joined)
	}
	return nil
}

func (w *walker) walk(root string) error {
	err := readDir(root, w.onDirEnt)
	if err != nil {
		return err
	}
	return err
}
