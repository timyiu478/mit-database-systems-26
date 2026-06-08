## 2PC

* Assumptions:
    * network: asynchronous
    * process: fail-stop with recovery
* combine with ARIES:
    * worker/subordinator: log and flush local decsion (prepared/aborted) before voting year/no
    * coordinator:
        * log and flush the final decision before returning to the caller 
    * log info/what logs need to force-flush depends on preassumption (presume abort/presume commit)
        * require co-design with both coordinator and worker
    * recovery:
        * worker in prepared state -> polling coordinator about the final decision
        * coordinator after before final decision: answer abort
        * coordinator after writing final decision: answer the logged final decision
* combine with Consensus algorithm: 
    * why? keep the liveness by making coordinator HA
