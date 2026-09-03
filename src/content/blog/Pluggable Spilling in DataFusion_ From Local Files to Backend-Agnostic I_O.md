---
title: "Pluggable Spilling in DataFusion: From Local Files to Backend-Agnostic I/O"
description: "A first-person account of tracing a PostgreSQL BufFile integration problem through DataFusion's spill architecture and turning the existing local-file implementation into a pluggable backend interface."
pubDate: "2026-09-03"
tags:
  - rust
  - apache-datafusion
  - databases
  - async-io
  - open-source
---

Hello Everyone, I’m Shrey. I’m writing these posts to document my work and turn individual engineering tasks into stories that I can explain clearly later. This one is about a contribution to Apache DataFusion that started as an extension-point problem and grew into a change spanning the spill abstraction, Arrow IPC, asynchronous reads, memory accounting, and parts of Sort-Merge Join.

I was still fairly early in my software-engineering journey (still am) when I worked on this. What made the contribution valuable was not just the final API. It was the process of finding where the original assumption lived, proposing something that seemed reasonable, having reviewers point out where it would break, and then following those problems through the code until the design became more general.

DataFusion’s spill files were built around local filesystem paths. PostgreSQL’s `BufFile` mechanism does not expose an ordinary path. That became a problem while I was trying to contribute support for `BufFile`-backed spilling to ParadeDB.[1]

This is the story of how I traced that problem upstream, how the proposed extension point changed, and why the final contribution ended up being more than “replace one file type with a trait.”

## Summary

The final change introduced three interfaces: `SpillFile`, `SpillWriter`, and `TempFileFactory`. It changed the internal spill type from `RefCountedTempFile` to `Arc<dyn SpillFile>` and allowed `DiskManager` to create backend-specific spill files.

The final design uses asynchronous reads and synchronous writes. Reads expose an asynchronous stream of `Bytes`; writes continue through `std::io::Write` because Arrow’s IPC writer already uses that boundary. The sections below explain how the design arrived there.

## How I encountered the problem

I wanted to contribute to ParadeDB’s issue about adding `BufFile`-backed spilling. PostgreSQL extensions have to work within PostgreSQL’s own resource-management model, including temporary tablespaces, `temp_file_limit`, and cleanup tied to PostgreSQL’s execution lifecycle.[1]

While investigating how ParadeDB could provide a different spilling backend, I started looking for the extension point in DataFusion. I initially expected that the main task would be to configure `DiskManager` with a different temporary-file creator. That was only part of the problem.

The existing DataFusion code did not just ask a manager to create a temporary file. Spill consumers also assumed that the result had a normal filesystem path that could be opened again later. A PostgreSQL `BufFile` is managed by PostgreSQL and intentionally does not have to look like an ordinary OS file.

That distinction was the important discovery: changing the factory alone would not be enough if the rest of the execution code continued to reopen paths directly.

## How spilling worked before the refactor

Before this work, the local spill path was centered around [`DiskManager`](https://docs.rs/datafusion/latest/datafusion/execution/disk_manager/struct.DiskManager.html) and [`RefCountedTempFile`](https://docs.rs/datafusion/latest/datafusion/execution/disk_manager/struct.RefCountedTempFile.html). When an operator needed to spill intermediate query data, `DiskManager` created and tracked a local temporary file. The reference-counted wrapper allowed different parts of query execution to hold the same spill file while its underlying resource remained alive until the last reference was released.

The read and write paths were still local-file paths, though. In simplified form, a consumer could do something like:

```rust
let path = spill_file.path();
let file = File::open(path)?;
```


This was a sensible design for local spilling. The problem was that the storage backend had become part of the assumptions made by the consumers. A PostgreSQL-managed temporary file or an object-store upload cannot necessarily provide a path that DataFusion can reopen.

That is why a new factory by itself would not solve the integration. The extension point had to surround the spill file operations as well as spill-file creation.

## The first proposal: use standard I/O traits

My first instinct was to keep the local implementation and replace direct path usage with ordinary Rust I/O traits. The initial shape was deliberately conventional:

```rust
pub trait SpillFile: Send + Sync {
    fn size(&self) -> u64;

    // Useful for local files, but absent for pathless backends.
    fn path(&self) -> Option<&Path> {
        None
    }

    fn open_write(&self) -> Result<Box<dyn std::io::Write + Send>>;
    fn open_read(&self) -> Result<Box<dyn std::io::Read + Send>>;
}

pub trait TempFileFactory: Send + Sync {
    fn create_temp_file(
        &self,
        request_description: &str,
    ) -> Result<Arc<dyn SpillFile>>;
}
```

This solved the immediate path problem. A backend could implement `open_read()` and `open_write()` without exposing a path, while the local implementation could continue to provide one for debugging and compatibility.

It also gave `DiskManager` a clear extension point:

```rust
pub enum DiskManagerMode {
    OsTmpDirectory,
    Directories(Vec<PathBuf>),
    Custom(Arc<dyn TempFileFactory>),
    Disabled,
}
```

At that point, I was still thinking about both directions as ordinary `Read` and `Write` operations. That was the part the review made me reconsider.

## Why the first proposal was not enough

The first issue was that `std::io::Read` and `std::io::Write` are synchronous interfaces. They work naturally for local files, but they are not a good universal boundary for a backend whose natural operations are asynchronous. An object store or a database-managed temporary-file service may need to wait on I/O rather than block a thread.

The old DataFusion read path already showed why this mattered. The spill manager ultimately exposed an asynchronous stream of record batches, but the underlying implementation started from a synchronous file reader and a pull-based Arrow IPC reader. That mismatch required a hand-written state machine. The reader performed blocking work in spawned blocking tasks and returned one batch at a time.

The comment on the original implementation described the reason for that structure:

> Stream that reads spill files from disk where each batch is read in a spawned blocking task. It will read one batch at a time and will not do any buffering, to buffer data use `spawn_buffered`.A simpler solution would be spawning a long-running blocking task for each file read. This approach does not work because when the number of concurrent reads exceeds the Tokio thread pool limit, deadlocks can occur and block progress.

The problem was not just that the reader was synchronous. It was that synchronous reading had been pushed through an asynchronous interface using a state machine and a limited blocking thread pool. The existing approach avoided one failure mode, but it made the read path harder to reason about and left the backend abstraction tied to local blocking I/O.[3]

The second issue was buffer ownership. A writer that accepts a borrowed slice cannot hold that slice after the call returns unless it copies the data. A backend that wants to queue chunks for an asynchronous upload task needs an owned value that can outlive the call.

That is where `Bytes` became relevant. Arrow uses `Bytes` in places where shared ownership of an immutable byte region is useful. It can be cloned and passed between components without copying the underlying contents each time. It does not make every operation zero-copy, but it gives an asynchronous backend a better ownership boundary than a borrowed `&[u8]`.

I explored a writer shaped roughly like this:

```rust
pub trait SpillWriter: Send {
    fn write(&mut self, data: Bytes) -> Result<()>;
    fn flush(&mut self) -> Result<()>;
    fn finish(&mut self) -> Result<()>;
}
```

For reading, the direction was clearer:

```rust
fn read_stream(
    &self,
) -> Result<Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>>>;
```

The new system could then be asynchronous and push-based from the beginning rather than starting with a synchronous reader and adapting it into a stream.

## The Arrow IPC part: moving from pull to push

The asynchronous read path used Arrow’s `StreamDecoder`. The decoder accepts incoming byte chunks and produces decoded record batches. That fit the new spill interface better than opening a synchronous `StreamReader` for each file.

There was one missing Arrow feature. DataFusion controls the IPC data that it writes for its own spill files, so it can use a trusted-input mode that skips validation. At the time, `StreamDecoder` did not expose the same `with_skip_validation` option available in related readers. I opened a separate Arrow change to add it.[2]

```rust
/// # Safety
///
/// This is only sound when the input is trusted and known to be valid
/// Arrow IPC stream data.
pub unsafe fn with_skip_validation(
    mut self,
    skip_validation: bool,
) -> Self {
    unsafe { self.skip_validation.set(skip_validation) };
    self
}
```

The schema check also had to move because the old reader and the new decoder learned about the schema at different times. Previously, code could inspect the schema directly from the reader:

```rust
// Validate the schema read from the Arrow IPC file.
let actual_schema = reader.schema();
```

With a push-based decoder, the IPC header arrives as part of the byte stream. The schema is available only after the first batch has been decoded, so validation moved to that point:

```rust
// One-time schema validation on the first decoded batch.
// The IPC stream embeds the writer's schema in its header;
// StreamDecoder surfaces it via the first batch's schema.
// We check here rather than in new() because schema bytes
// only arrive after decoding the IPC header from the stream.
```

That was a small-looking change, but it was part of converting the read path from “construct a reader and ask it for metadata” to “feed bytes into a decoder and react as data arrives.”

The trade-off was that the decoder already had a scratch buffer, while the asynchronous source also produced chunks. On each poll, bytes from the stream have to be made available to the decoder, and newly received bytes are held in the stream-side buffer until they can be consumed. The design removes the old blocking-reader state machine, but it does not make buffering disappear.

A reviewer called out exactly that concern during discussion: decoding now required an additional copy or buffering step. That concern was valid. The question became whether the extra buffering was acceptable in exchange for a backend-neutral asynchronous read path.

## The final writer decision: keep writes synchronous

I tried to find an asynchronous alternative to Arrow’s IPC writer, but the writer used by this path accepted `std::io::Write`, and there was no equivalent asynchronous writer available for me to use.

My first attempt was to preserve a uniform async-facing abstraction by adding an adapter. The adapter accepted the synchronous bytes from Arrow, converted or buffered them as needed, and forwarded them to an asynchronous writer. That made the interface look symmetric, but it introduced another copy or buffering boundary in the write path.

After review, the better choice was to keep the synchronous boundary where Arrow already had one. A custom backend can decide what to do internally: write synchronously, copy into an owned buffer, queue work to another task, or run the synchronous operation in a blocking thread. DataFusion does not need to impose one of those policies on every backend.

The final writer interface became:

```rust
pub trait SpillWriter: std::io::Write + Send {
    /// Allows a backend to flush, close, or commit its resource.
    fn finish(&mut self) -> Result<()>;
}
```

The final split was therefore:

| Direction | Interface | Reason |
| --- | --- | --- |
| Write | `std::io::Write` plus `finish()` | Arrow’s IPC writer already uses synchronous `Write`. |
| Read | `Stream<Item = Result<Bytes>>` | Backends may need native asynchronous reads. |
| Decode | `StreamDecoder` | It consumes pushed byte chunks and produces record batches. |

The asynchronous writer question remained follow-up work and was tracked separately in [5].

## The final API and the factory extension point

The interfaces after review looked like this:

```rust
pub trait SpillFile: Send + Sync {
    /// Returns an OS path for local files, or None otherwise.
    fn path(&self) -> Option<&Path> {
        None
    }

    /// Returns the current size when cheaply available.
    fn size(&self) -> Option<u64>;

    /// Returns the contents as an asynchronous byte stream.
    fn read_stream(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>>>;

    /// Opens a writer for appending serialized data.
    fn open_writer(&self) -> Result<Box<dyn SpillWriter>>;
}

pub trait SpillWriter: std::io::Write + Send {
    fn finish(&mut self) -> Result<()>;
}

pub trait TempFileFactory: Send + Sync {
    fn create_temp_file(
        &self,
        request_description: &str,
    ) -> Result<Arc<dyn SpillFile>>;
}
```

The `TempFileFactory` is the part that makes the backend choice extensible. `DiskManager` continues to own the general operation of creating temporary spill files, but it can delegate the concrete resource creation to an implementation supplied by the caller.

That factory pattern is useful here because DataFusion does not need to know whether the implementation is backed by an OS file, a PostgreSQL `BufFile`, or an object store. Each backend can implement the same lifecycle while keeping its own ownership, quota, buffering, and cleanup rules.


## Implementing the local backend

The first DataFusion implementation changed the internal spill type from `RefCountedTempFile` to `Arc<dyn SpillFile>`. The goal was not to throw away the local implementation. It was to make the existing local backend satisfy the new contract while allowing a PostgreSQL or object-store backend to provide another implementation.[4]

Conceptually, the local implementation became:

```rust
impl SpillFile for RefCountedTempFile {
    fn path(&self) -> Option<&Path> {
        Some(self.tempfile.path())
    }

    fn size(&self) -> Option<u64> {
        Some(self.current_disk_usage())
    }

    fn open_writer(&self) -> Result<Box<dyn SpillWriter>> {
        let file = self.tempfile.as_file().try_clone()?;
        Ok(Box::new(FileSpillWriter::new(
            file,
            Arc::clone(&self.disk_manager),
            Arc::clone(&self.current_file_disk_usage),
        )))
    }

    fn read_stream(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>>> {
        // The local backend uses tokio::fs::File and ReaderStream.
        todo!()
    }
}
```

`path()` remained available for local files and debugging, but callers were expected to use `open_writer()` and `read_stream()` instead of reopening the path themselves.

## The read path in practice

The local read implementation uses an asynchronous file stream and feeds chunks into `StreamDecoder`

This was a real improvement over the old “synchronous reader inside an async stream” arrangement. The hand-written state machine existed because the old path had to bridge those two models. With a push-based stream, each `poll_next()` can ask the byte stream for more data, pass available chunks to the decoder, and return when the decoder produces a batch or the source yields `Poll::Pending`.

It was not free. The old `StreamReader` had its own parsing buffer. The new path had a stream-side buffer and the decoder’s scratch buffer. I had to treat that extra movement of bytes as an explicit trade-off rather than casually calling the new path zero-copy.

The buffer size was found through benchmarking rather than designed from first principles. The initial small `ReaderStream` buffer made the `spill_io` benchmark look worse. I tried a buffer around 1 MiB, but values around 256 KiB began failing SQL logic tests. The value that worked for the pushed change was 128 KiB:

```rust
ReaderStream::with_capacity(file, 128 * 1024)
```

The working hypothesis was that the old reader often consumed a complete IPC frame without yielding, while the new stream could yield repeatedly after consuming its initial buffer. A larger buffer reduced that overhead without destabilizing the tests.

## A partial Sort-Merge Join migration

The core spill abstraction was not the only place that assumed local synchronous files. Two spill-reading paths in Sort-Merge Join:`materializing_stream.rs` and `bitwise_stream.rs`:were still using direct file access or `open_sync_reader()`. They were not using the SpillManager API to obtain the new asynchronous spill stream.[4]

Thankfully, the change was limited to the spill-reading portions in these files that bypassed the abstraction and not the entire Sort-Merge Join operator. 

By this point, a significant amount of time had already gone into the pluggable-spill PR. I initially considered adding an `open_sync_reader()` escape hatch to `SpillFile` as an interim step. That would have let the main change merge while leaving the two Sort-Merge Join paths for later. It would also have left a synchronous method in an abstraction intended to support pathless and asynchronous backends.

After a brief back-and-forth with Andrew, I decided to start working on the Sort-Merge Join migration in parallel. The same shortcut-versus-fix decision appeared here as it had in the writer design: keeping the escape hatch would have made the first merge easier, but it would have preserved the old assumption in the new API.

I then changed the affected paths in parallel. In `materializing_stream.rs`, spilled `BufferedBatch` values were restored through the SpillManager’s asynchronous spill-stream API before the materialized state was frozen. In `bitwise_stream.rs`, the old synchronous reads were replaced with polling of the `SendableRecordBatchStream` returned by `spill_manager.read_spill_as_stream(...)`. Because the code already ran inside `poll_next()`, the active stream and progress state had to remain in the operator across `Poll::Pending`.

A synchronous `for` loop inside `poll_next()` cannot simply become `.await`. The important state had to be stored explicitly and resumed later. That was the main async-Rust lesson in this part of the change.

The migration also required checking empty-stream and error invariants. An immediately empty spill stream was not silently treated as successful data restoration. Existing behavior was preserved while the filtered-join spill fuzz-test failure found during review was fixed. Dedicated coverage for a spill stream yielding `Poll::Pending` remained useful follow-up work because the old code did not provide a clean injection point for a mock stream.

The Sort-Merge Join work was developed in [DataFusion #22230][4], opened in parallel with the pluggable-spill change, and later rebased so it could be reviewed and merged separately.

## Tracking the bytes that are actually written

The old implementation required callers to update disk usage after modifying a temporary file. That worked for local files, but it made accounting depend on every caller remembering to perform the update. It also relied on filesystem metadata that a custom backend might not have.

The new local writer updates usage at the writer boundary:

```rust
impl std::io::Write for FileSpillWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let len = buf.len() as u64;
        let new_global = self
            .disk_manager
            .used_disk_space
            .fetch_add(len, Ordering::Relaxed)
            + len;

        if new_global > self.disk_manager.max_temp_directory_size() {
            self.disk_manager
                .used_disk_space
                .fetch_sub(len, Ordering::Relaxed);

            return Err(std::io::Error::other(
                "spill disk-usage limit exceeded",
            ));
        }

        self.file.write_all(buf)?;
        self.current_file_disk_usage
            .fetch_add(len, Ordering::Relaxed);

        Ok(buf.len())
    }
}
```

Counting at this boundary measures serialized IPC bytes rather than the in-memory size of the `RecordBatch`. That distinction matters because the number of bytes written can differ from `RecordBatch::get_array_memory_size()`.

A quota failure also had different meanings depending on where it occurred. A failure while memory pressure is starting the spill process may be part of the logic that decides how to recover. A failure after a spill writer has already started has no equivalent fallback: the query has failed to write its temporary data. The final implementation let that failure cross the `std::io::Error` boundary with a descriptive message.

## Testing the invariants

The most useful tests were not tests of exact allocation counts. They checked the behavior that the abstraction promised.

A repartition test initially used a very small memory limit to force spilling while checking preserve-order output. The new writer path added enough setup overhead that the old limit could fail before the intended spill. Raising the limit could introduce the opposite problem: producers might consume the available memory first and the test might finish without spilling.

The test was trying to control too much implementation detail. The useful properties were simpler:

```rust
assert!(spill_count > 0);
assert!(output_is_sorted(&batches));
```

The disk-manager tests also moved behind the public abstraction:

```rust
let file = disk_manager.create_tmp_file("test")?;
let mut writer = file.open_writer()?;

assert_eq!(disk_manager.used_disk_space(), 0);
writer.write_all(b"hello world")?;

let usage = file.size().unwrap();
assert!(usage > 0);
assert_eq!(disk_manager.used_disk_space(), usage);
```

Clone tests checked that several `Arc<dyn SpillFile>` references shared accounting. Dropping one reference must not subtract usage while other references remain. Usage should return to zero only after the final reference is dropped.

A rollback test checked that a failed write did not permanently inflate the global usage counter.

## Review, follow-up work, and merge

The review covered API compatibility, allocation behavior, error semantics, buffering, and benchmark results. It also resulted in an upgrade-guide entry because downstream code could be affected by the new public types and disk-manager mode.[3]

The maintainers asked for a concrete example of a user-defined backend. An ObjectStore-backed example was developed separately in [6]. That example was useful because it tested whether the abstraction supported a backend that did not naturally behave like a local file.

The main pluggable-spill change eventually merged as [3]. The separate Sort-Merge Join migration made the core change easier to review, while the object-store example demonstrated how a downstream backend could use the extension point.

I also got a mention in the DataFusion 55 release blog, which was a nice outcome after spending so much time in low-level implementation and review details.

## A small note about the blog

One of the enjoyable parts of this work was not only writing the code. It was triaging the problem, coming up with a solution, finding the places where the solution did not fit, and discussing those details with reviewers and maintainers. The review process changed the API for the better, but it was also genuinely fun to interact with people who care about the same systems problems.

I am writing this as someone who is still building experience as a software engineer and learning how to contribute to large open-source projects. If any part of the explanation is unclear or technically inaccurate, feedback is welcome. If you are interested in Rust, Arrow, query engines, or database internals, the [8] is a good place to start.

## References

[1]: https://github.com/paradedb/paradedb/issues/4064 "ParadeDB issue : JOINs: Add Support for Spilling to Disk"

[2]: https://github.com/apache/arrow-rs/pull/9749 "arrow-rs #9749 : Add with_skip_validation to StreamDecoder"

[3]: https://github.com/apache/datafusion/pull/21882 "DataFusion #21882 : Introduce pluggable SpillFile and TempFileFactory"

[4]: https://github.com/apache/datafusion/pull/22230 "DataFusion #22230 : Update SortMergeJoin to use async spill abstractions"

[5]: https://github.com/apache/datafusion/issues/23247 "DataFusion #23247 : Add an async API for spill file writing"

[6]: https://github.com/apache/datafusion/pull/23170 "DataFusion #23170 : ObjectStore-backed TempFileFactory / spill example"

[7]: https://datafusion.apache.org/blog/output/2026/08/25/datafusion-55.0.0/#pluggable-spill-backends "Apache DataFusion blog"

[8]: https://discord.com/channels/885562378132000778/885562378132000781 "Apache DataFusion community"

1. [ParadeDB issue #4064 — JOINs: Add Support for Spilling to Disk](https://github.com/paradedb/paradedb/issues/4064)

2. [arrow-rs #9749 — Add with_skip_validation to StreamDecoder](https://github.com/apache/arrow-rs/pull/9749)

3. [DataFusion #21882 — Introduce pluggable SpillFile and TempFileFactory](https://github.com/apache/datafusion/pull/21882)

4. [DataFusion #22230 — Update SortMergeJoin to use async spill abstractions](https://github.com/apache/datafusion/pull/22230)

5. [DataFusion #23247 — Add an async API for spill file writing](https://github.com/apache/datafusion/issues/23247)

6. [DataFusion #23170 — ObjectStore-backed TempFileFactory / spill example](https://github.com/apache/datafusion/pull/23170)

7. [Apache DataFusion blog](https://datafusion.apache.org/blog/output/2026/08/25/datafusion-55.0.0/#pluggable-spill-backends)

8. [Apache DataFusion community](https://discord.com/channels/885562378132000778/885562378132000781)

---
