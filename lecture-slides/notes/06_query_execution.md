Iterator Model Advantage:

* By processing tuples incrementally, pipelining removes the overhead of writing intermediate results to temporary storage
    * reduce memory and disk I/O
    * cache locality: result can stay in CPU cache
    * low latency of getting the first row of query result

Iterator Model vs Materialize Model vs Vectorization Model:

* Iterator: operator emits 1 tuple at a time
* Materialize: operator emits ALL tuples at once
* Vectorization: operator emits BATCH tuple at a time
