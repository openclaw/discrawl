# SQLite writer durability

Persistent archive and metrics writers use `synchronous=FULL`. On macOS they
also request `fullfsync=1` and `checkpoint_fullfsync=1`. The settings belong to
the private writer URI, so each physical connection opened by the pool receives
them, including replacement connections. Writer startup checks the effective
settings and fails on an error or mismatch. Read-only connections are unchanged.

The pinned modernc SQLite driver applies `_synchronous=FULL` after Crawlkit's
`_pragma=synchronous(NORMAL)` default. Crawlkit still owns WAL mode, transaction
boundaries, busy timeout and pooling. This change introduces no schema migration,
capture feed activation, new schedule or credential access. Database files are
created privately before opening the URI; existing files retain the previous
0600 permission policy.

FULL synchronizes WAL commits; macOS full-sync pragmas request the stronger
platform flush where supported by SQLite and the filesystem. Durability still
depends on the operating system, storage device and controller honoring flushes.
A process-kill recovery test is not a physical power-loss certification.

Validation covers initial, simultaneous and replacement writer connections,
read-only rejection of writes, special-character paths and private creation.
Deployment acceptance additionally kills a disposable instance during an
uncommitted transaction, verifies prior commits and rollback, and reopens with
the signed runtime. Live collector processes are changed only at the deliberate
deployment boundary; retained data, WAL/SHM, cursors and held migrations remain
intact.
