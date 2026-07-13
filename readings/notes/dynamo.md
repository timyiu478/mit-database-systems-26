---
title: "Dynamo: Amazon’s Highly Available Key-value Store"
tags: ["Distributed Storage", "NoSQL", "Eventual Consistency", "Peer-to-Peer"]
reference: https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf
---

# Oneline Summary

Dynamo is an eventually consistent, peer-to-peer (P2P), highly available key-value storage system that uses object versioning and reconciles data conflicts during reads.

# Contributions

* show eventual consistency in global scaled production system can work
* how to tune the key techniques to achieve the stricut performance requirements

# Design Considerations

* AWS services
    * single key access, no relation with other keys
        * e.g. best seller list
    * different requirements between consistency and availability
    * shopping cart: need to store multiple versions of an object
* Global Scale: component failure is norm 

# System Requirements

* Key-value/get-put interface
* Single object update
* ACID: no isolation
* SLA: In 99.9 percentile, a respone will be return withn 300 ms where the peak load is 500 req/sec 
* Incremental scalability
* Low administrative overhead
* Heterogeneity: the work Distribution is proportional to the capability of the instance

# Key Ideas

* Partition: Consistent Hashing => increment scalability
* Membership: Gossip => no centralized registry, preserve symmetry
* Conflict Reconciliation: Reconciliation during read => allow conflict writes, version size is bounded by nodes (not update rate)
* Handle Temporary Failure: Sloppy Quorum, Hinted Handoff => HA even when some nodes are failed
* Recovery from permanent Failure: Merkle Tree => reduce the amount of data needs to be transfered

# Tuning the key techniques

* Consistent Hashing: virtual nodes
* Quorum: allow sloppy Quorum, object buffer to trade durability for performance, tunable N,R,W paramenters

---

# Details

## Consistent Hashing

* Pros
   * the node departure or arrival only affect the immediate neighbors 
* Challenges
    * load imbalance
        * fix: virtual node: each node maps to list of virtual nodes
            * when the node fails, the workload of the failed node is evenly distributed to the remaining live nodes
            * when a new node is added, the new node consumes rougly the same amout of work from each existing nodes
            * Heterogeneity: the capability of the instance decides the number of virtual nodes  

## Replication

* Challenges
    * Because of virtual nodes, the first N successor positions for particular key may be owned by less than N distinct physical nodes.
        * fix: the preference list is constructed by skipping some posistions in a ring to ensure the list contains N distinct physical nodes.

## Data Versioning

* Object versioning: each version of an object is a vector clock and the coordinator of the object is the node in vector clock
    * allow us to distinguish (non-)concurrent versions 
        * non-concurrent versions can be forgotten
        * can store mutliple concurrent/conflict versions and let user to reconcile them
* Limit the size of vector clock by clock truncation:
    * why truncation is needed?
        * new nodes join, network partition => increase the size of vector clock
    * problem: the descendant relationships cannot be derived accurately.
        * this issue has not been thoroughly investigated. 

## Handling Permanent Failure: Node synchronization

* Merkle Tree per key range
    * cons
        * need to recalculate the tree when the key range is changed

## Experience and Lesson Learned

* How to trade durability for performance
    * object buffer: response read by getting data from buffer (no disk access, not limited by slowest R/W replicas)
        * Reduce durability risk: the system gets its W acknowledgments rapidly from the buffered nodes, while one single replica slowly performs a forced-disk write in parallel

* Load Distribution
    * **popular** key should evenly distributed across nodes

---

# Questions

## Q. why when a client wishes to update an object, it must specify which version is updating? 

Without the version context from the client, Dynamo would be forced to guess(overwrite or create a new branch?)

## Q. why the pro of anti-entropy using merkle tree is synchronizes divergent replicas in the background?

Because the merkle tree makes the recovery process cheap. It allows the recovery process runs in background without steal resources from read/write path.

---

# TODO

* read section 6
