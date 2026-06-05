# Multi-Version Concurrency Control

## Benefit of MVCC

* reader does not block writer.
* writer does not block reader.

## Problem of MVCC

* Snapshot Isolation is susecptible about write skew anomaly.


## Q. Why do we have to combine MVCC with 2PL/OCC?

MVCC does not handle Write-Write conflict.

