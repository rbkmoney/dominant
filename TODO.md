# Alpha

* Evict cache entries in order to provide bounded memory usage
    * LRU cache?
    * Hard limits
* Bound computational complexity
    * Context-aware machinegun?
    * Periodical history snapshotting?
    * Reuse cache entries where possible

# Release

* Simplify cache interfaces
* Encode machine events and responses with a schema-aware protocol
    * Thrift / compact protocol?
* Fix potential race when a `Repository` request getting processed earlier than the start machine request being issued.
