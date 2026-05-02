---
tags:
  - concurrency
  - distributed-systems
  - database
  - atomicity
  - isolation
title: Concurrency Control and Recovery by Michael J. Franklin
description:
---

## Takeaways

---

## Summary

* Atomicity via Logging
* Isolation via 2-Phrase Locking

---

## Q & A

***Q. What failure models are we dealing with in this paper?***

* **Transaction Failure.** The system has to roll back all changes made by the aborted transaction to *preserve the atomicity property*.
* **System/Memory Failure.** The updates of the committed transactions should be reflected and the updates of the other types of transactions should be removed in the new database instance. 
* **Media/Disk Failure**. The online version of the data is lost. The data needs to be restored from the backup.


***Q. Under what circumstances would you want transaction executions to respect the ACID properties?***

* **Payment transfer from account A to account B.** We need the ACID properties to ensure the invariant that the sum of the balance of A and B before transaction execution must equal the sum of the balance of A and B after transaction execution.


***Q. Are there systems that don’t need to have all four (ACID) properties?***

* **Collaborative Docs**. These systems do not need the *isolation* property.
* **Caching systems.** These systems do not need the *persistence* property. 

***Q. What is an example from the paper that illustrates the trade-off between implementing ACID transaction properties and maintaining good performance?***

***Q. How does that policy or technique trade off performance?***

***Q. Why would you use this policy or technique? (In what context, under what circumstances, etc.)***

---

## Details
