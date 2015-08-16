
Build and Run:

```
go build
./ttrack < <input file>
```

Design

The design is based on a stack of frames, where each frame represents the set of
values in the current transactional context.   The root frame can be thought of
as the special purpose frame beyond (or above) which there are no more
transactions.   All operations within a transaction modify values that are only
visible within that transaction.  Also for each value, a time stamp is also kept
so that  if we have concurrent writes to the data store, we could use the time
stamp of values across the different transactions to handle conflicts.

Two maps are stored in each frame - one map for the value for a given key (along
with a flag that marks whether the value is deleted in that context) and another
map for a reference counts of all values.


On a GET, the current frame is looked up for the value of a key.  If an entry
does not exist, its parent is searched and so on.  Also this value is now cached
in the latest frame going forward.

On a SET, the lookup is performed as in a GET, but a similar lookup is performed
for the value's ref count in the current frame and its ancestors (and cached if
found).  Then the new value's refcount is incremented in the current frame only
and the old value's refcount is decremented (also only in the current frame).

UNSET is similar to a SET in that the appropriate lookups are performed but a
value's Deleted flag is set to true instead of actually being removed.  This has
the benefit of not requiring a ancestral lookup again (if we are in a
transaction).  Also same with the refcount of the value.

BEGIN simply pushes a new stack frame where operations are recorded.  As an
optimisation the new push only happens if the current frmea is the root frame or
if it has atleast one change in it.

On a ROLLBACK the current frame is disposed, leaving the values in the previous
frame intact.

On a COMMIT, each from starting from the top is merged with its parent until
only the root frame is left.  Merging involves overriding the parent's value for
a key (and count for the value) with what was found in the frame (if it was).
Values that were not referenced in the current transaction wont be affected.
The timestamp in each value is useful here to detect writes by concurrent
clients that are not valid (though not implemented for now as DB is single
threaded).

The state of the DB can be printed with new "DEBUG" command.
