# DBMS

An ACID-compliant, thread-safe database management system implemented in Go. Utilizes b+tree and extendible hash indexing, supporting logical and bloom filter query optimizations. Implemented a write-ahead log (WAL) for fault tolerance/crash recovery. Utilized hand-over-hand fine-grained locking to handle high-volume concurrent read/writes.
