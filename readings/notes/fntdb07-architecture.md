---
title: Architecture of a Database System
reference: https://dsf.berkeley.edu/papers/fntdb07-architecture.pdf
tags: ["Design Principles", "System Architecture"]
---

# Goal of this paper

Presents the architecture disscusion of DBMS design principles including
process models, parrallel architecture, storage system design, transaction
system implementation, query processor and optimator architecture, and typical
shared components and utilies.

---

# Section 1: Introduction

High-level view of the life of the query:

![](assets/the_life_of_query_high_level.png)

---

# Section 2: Process Models

Key decision that the multi-user system needs to make: How the exeuction of the concurrent user requests ared mapped to the operating system processes or threads?

1:1 mapping exits between DBMS worker and DBMS client.

Assumptions:

* OS kernel thread memory overhead is small and context switch is inexpensive
* Uniprocessor hardware

Process Models: Process per DBMS worker, Thread per DBMS worker, and Process pool.

![](assets/dbms_process_models_in_uni_processor.png)

Postgres uses process models: https://www.interdb.jp/pg/pgsql02/01.html

Client communication buffers: SQL is typically used in a **pull model**: clients consume result tuples from a query cursor by repeatedly issuing the SQL FETCH request.

Admission Control Policy: does not accept rew work until DBMS has enough resource to process it.

* Tier 1: keep # of client connections under a threshold
* Tier 2: query optimizer estimates the workload of the query and decide whether postpone the exeuction of the query

---

# Section 3: Parallel Architecture: Processes and Memory Coordination


---

# Section 4: Relational Query Processor


---

# Questions

Q. OS threads vs Lightweight thread package

OS threads are scheduled by kernel scheduler.
Lightweight threads are scheduled by application level thread scheudler.
Lightweight threads in kernel POV is a single thread of execution.

* pros: no mode switch for scheduling
* cons: no I/O concurrency in synchronous I/O
