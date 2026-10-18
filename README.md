# ChronicleQueue (ZeroMesa)

A high-throughput, low-level streaming storage engine built in Java 21 from scratch.

## Architecture
- **Append-Only Write-Ahead Log (WAL)** using binary frame serialization and zero heap copying.
- **Memory-Mapped Indexing (mmap)**: Bypassing OS user-space context switches for sparse index binary searches.
- **Lock-Free Ring Buffer**: Mechanical sympathy and atomic position coordination.
- **Zero-Copy Network Pipeline**: Kernel-level data streaming via FileChannel.transferTo().
