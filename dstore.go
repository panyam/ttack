package main

import "fmt"

type Entry struct {
	/**
	 * Time stamp of the entry.
	 */
	TimeStamp int64

	/**
	 * The value for the entry
	 */
	Value int64

	/**
	 * The status of the entry
	 */
	Deleted bool
}

type Frame struct {
	/**
	 * Key to value mapping
	 */
	context map[string]Entry

	/**
	 * Value to count mapping
	 */
	counts map[int64]int

	/**
	 * The parent from which this frame is created to do a transaction
	 */
	parent *Frame
}

type MemDataStore struct {
	currFrame *Frame
}

func NewFrame(parent *Frame) *Frame {
	var frame Frame
	frame.context = make(map[string]Entry)
	frame.counts = make(map[int64]int)
	frame.parent = parent
	return &frame
}

/**
 * Finds the entry corresponding to a particular key in the current frame.
 * If it does not exist it is fetched from the top most ancestor frame and
 * copied over.  By only doing this lookup only when it is required, we
 * ensure that the entire frame does not need to be copied.  And by
 * copying on a miss, this ensures subsequent calls dont incur an
 * entire traversal of the parent stack.  Also if/when we do concurrent
 * accesses, copying of entries means the timestams can be recorded
 * to prevent bad merges on writes.
 */
func (frame *Frame) lookupEntry(key string) (Entry, bool) {
	entry := Entry{}
	for curr := frame; curr != nil; {
		entry, found := curr.context[key]
		if found {
			frame.context[key] = entry
			return entry, true
		}
		curr = curr.parent
	}
	return entry, false
}

/**
 * Similar to lookupEntry but for the frequencies of a particular value over
 * time.
 */
func (frame *Frame) lookupValue(value int64) int {
	for curr := frame; curr != nil; {
		count, found := curr.counts[value]
		if found {
			frame.counts[value] = count
			return count
		}
		curr = curr.parent
	}
	frame.counts[value] = 0
	return 0
}

/**
 * Increments the ref count of a particular value
 */
func (frame *Frame) incrementCount(value int64) {
	// update value count
	frame.counts[value]++
}

/**
 * Decrements the ref count of a particular value
 */
func (frame *Frame) decrementCount(value int64) {
	// update value count
	count := frame.lookupValue(value)
	if count > 0 {
		frame.counts[value] = count - 1
	}
}

func NewDataStore() *MemDataStore {
	var ds MemDataStore
	ds.currFrame = NewFrame(nil)
	return &ds
}

/**
 * Gets the value from the DS.
 *
 * 1. Look up the entry in the current frame.  If it exists (ie Status != 0)
 *    return its value based on status (if Exists the value, if Deleted then none)
 * 2. If status == 0 then recursively look up the parent till one is found and
 *    copy it to each successive frame and then return
 */
func (ds *MemDataStore) Get(key string) (int64, bool) {
	entry, found := ds.currFrame.lookupEntry(key)
	if !found {
		return 0, false
	}
	return entry.Value, !entry.Deleted
}

/**
 * Sets the value for a key in the current frame.  Also updates the value
 * counts.
 */
func (ds *MemDataStore) Set(key string, value int64) {
	entry, found := ds.currFrame.lookupEntry(key)

	// increment new value count
	ds.currFrame.incrementCount(value)

	if found {
		ds.currFrame.decrementCount(entry.Value)
	}

	// set the entry for key
	entry.Deleted = false
	entry.Value = value
	entry.TimeStamp++
	ds.currFrame.context[key] = entry
}

func (ds *MemDataStore) Del(key string) {
	entry, found := ds.currFrame.lookupEntry(key)
	if found {
		ds.currFrame.decrementCount(entry.Value)
	}
	entry.Deleted = true
	ds.currFrame.context[key] = entry
}

func (ds *MemDataStore) Count(value int64) int {
	return ds.currFrame.lookupValue(value)
}

func (ds *MemDataStore) BeginTransaction() {
	if ds.currFrame.parent == nil || len(ds.currFrame.context) > 0 {
		// push a new frame for the values being modified in this transaction
		// As an optimisation do this only if we are at the top most frame
		// OR if there has been no changes in the current transaction
		ds.currFrame = NewFrame(ds.currFrame)
	}
}

func (ds *MemDataStore) RollbackTransaction() bool {
	if ds.currFrame.parent != nil {
		// No changes so just pop off the last frame if it is not already the root
		ds.currFrame = ds.currFrame.parent
		return true
	} else {
		return false
	}
}

func (ds *MemDataStore) CommitTransaction() {
	parent := ds.currFrame.parent
	for parent != nil {
		for key, entry := range ds.currFrame.context {
			// TODO: if we had concurrent access then the parent entry's
			// TimeStamp could be > entry's TS - so we should check that
			// but no concurrent access right now!
			parent.context[key] = entry
		}

		// Update the counts after all the ops in the current transaction
		// have finished
		for value, count := range ds.currFrame.counts {
			parent.counts[value] = count
		}

		// All changes have been applied so pop off the frame
		ds.currFrame = parent
		parent = ds.currFrame.parent
	}
}

func (ds *MemDataStore) PrintDebug() {
	frameCount := 0
	for curr := ds.currFrame; curr != nil; {
		frameCount++
		fmt.Println("Frame: ", frameCount)
		fmt.Println("	Entries: ", curr.context)
		fmt.Println("	Counts : ", curr.counts)
		curr = curr.parent
	}
}
