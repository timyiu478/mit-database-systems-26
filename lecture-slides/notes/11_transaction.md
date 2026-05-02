Goal of concurrency control: allow concurrent execution while preserving serial equivalence

In serial schedule T1 -> T2, T2 can see T1's writes.

View Serialisability:

![](assets/view_serializability.png)

Conflict Serializability:

![](assets/conflict_serialisability.png)

View vs Conflict Serialisability:

![](assets/view_vs_conflict_serializability.png)

2PL Cascading Abort & Strict 2PL:

![](assets/strict_2pl_and_2pl_cascading_abort.png)

Noted that Strict 2PL does not prevent deadlock.

Key takeaways:

* 2PL correctness intuition
* How does strict 2PL prevent cascading abort
