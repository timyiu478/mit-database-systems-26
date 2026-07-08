---
tags:
  - concurrency
  - distributed-systems
  - database
  - atomicity
  - isolation
title: Concurrency Control and Recovery by Michael J. Franklin
description: "Atomicity via Logging, Isolation via 2-Phrase Locking"
---

## Summary

* Atomicity via Logging
* Isolation via 2-Phrase Locking

---

## Q & A

### Q. What failure models are we dealing with in this paper?

* **Transaction Failure.** The system has to roll back all changes made by the aborted transaction to *preserve the atomicity property*.
* **System/Memory Failure.** The updates of the committed transactions should be reflected and the updates of the other types of transactions should be removed in the new database instance. 
* **Media/Disk Failure**. The online version of the data is lost. The data needs to be restored from the backup.

### Q. Under what circumstances would you want transaction executions to respect the ACID properties?

* **Payment transfer from account A to account B.** We need the ACID properties to ensure the invariant that the sum of the balance of A and B before transaction execution must equal the sum of the balance of A and B after transaction execution.

### Q. Are there systems that don’t need to have all four (ACID) properties?

* **Collaborative Docs**. These systems do not need the *isolation* property.
* **Caching systems.** These systems do not need the *persistence* property. 

### Q. What is an example from the paper that illustrates the trade-off between implementing ACID transaction properties and maintaining good performance?

The paper use buffer manager as an example to illustrates the trade-off between implementing ACID transaction properties and maintaining good performance.

### Q. How does that policy or technique trade off performance?

* STEAL:
    * UNDO is needed
* NO-FORCE:
    * REDO is needed
* NO-STEAL:
    * No UNDO
    * buffer pool starvation; limit the options of pages that can safely swap out
* FORCE:
    * increase the amount of synchronize I/O; increase the time of commit
    * No REDO

### Q. Why short duration read lock allow to see non-repeatable reads?

Tx 2 can perform a committed write after the first read of tx 1 and tx 1 can perform a read after tx 2 committed write.

```
w0[A] -> c0 -> r1[A] -> w2[A] -> c2 -> r1[A] -> c1
```

---

## Details

### Logging

* Log Sequence Number(LSN) in Page: it is used to tell the state of the data page; what logs are reflected in the data page.
* Types of Log:
    * Physical: before image | after image | data position of page X
        * pro: idempotent
    * Logical: high-level info only
        * e.g. insert (row info)
        * pro: less data written in log, hide many implementation details of redo/undo
    * Physiological: constraint to specific page, logical operation within page
        * e.g. insert (row info), page X
            * free space manipulation logic is not specified
* Write ahead logging: it provides enough information for recovery when STEA/NO-FORCE buffer manager policy is used. 

### Locking

#### Problems of short duration locking

If we use short duration locking, the following schedules can arise:

```
w0[A] -> w1[A] -> a0
```

When we abort transaction 0, we can't simple restore the before image of t0 because it will overwrite the changes of w1[A].

```
w0[A] -> w1[A] -> a1
```

When we abort transaction 1, we can't simple restore the before image of t1 because it will overwrite the changes of w0[A].
