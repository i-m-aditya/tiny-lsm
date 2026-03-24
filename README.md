# tiny-lsm

A compact **log-structured merge-tree (LSM)** key-value storage engine in Rust: memtables, WAL, SSTable blocks, merge iterators, compaction, manifest, and MVCC-oriented pieces you build out over the course of the [Mini-LSM](https://skyzh.github.io/mini-lsm) material.

## Attribution and original work

This codebase started from the **starter template** in **[Mini-LSM](https://github.com/skyzh/mini-lsm)** by **[Alex Chi Z](https://github.com/skyzh)** and contributors. The course book is at [skyzh.github.io/mini-lsm](https://skyzh.github.io/mini-lsm).

**tiny-lsm** is an independent repository: it is **not** the official Mini-LSM monorepo. Per-file **copyright notices** from the upstream project are retained in the sources.

## License

Licensed under the **Apache License, Version 2.0** (same as Mini-LSM). See [`LICENSE`](./LICENSE). Upstream attribution is summarized in [`NOTICE`](./NOTICE).

## Repository metadata

After you host this project on GitHub (or elsewhere), set `repository` in [`Cargo.toml`](./Cargo.toml) to your clone URL. You may point `homepage` at your repo while keeping `documentation` on the Mini-LSM book if you still follow the course.

## Build and test

```bash
cargo build
cargo test
```

Interactive CLI:

```bash
cargo run --bin tiny-lsm-cli -- --help
```

Compaction simulator binary:

```bash
cargo run --bin compaction-simulator -- --help
```
