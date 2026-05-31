Deadlock Prevention:

![](assets/deadlock_prevention.png)

Intention Lock:

![](assets/intention_lock.png)

Phantom read problem:

![](assets/phantom%20problem.png)

Index Locking:

* key gap represents values that do not currently exist in the database but could be inserted in the future

Optimistic/Timestamp Concurrency Control:

* In the context of this OCC rule, "Write Phase" effectively implies the combined block of the Validation Phase + Write Phase?
* Problems:
    * each transaction needs its own workspace => memory (copy) overload
    * validation/write phrase can be bottleneck
        * serial commit: process one transaction at a time
