# First Exchange Slice Cases

Spec source: `docs/design/first-in-contract-exchange-slice.md`

## Canonical Cases

- `passthrough single batch`: exchange preserves schema, row count, row values,
  and null positions for one incoming batch
- `passthrough multi batch fifo`: exchange preserves incoming batch and row
  order across multiple handoffs
- `bounded queue blocked resume`: exchange reports adopted blocked semantics when
  queue capacity is reached, then resumes forwarding once downstream demand
  returns
- `finished after drain`: exchange reports finished only after upstream is
  finished and buffered batches are fully drained
- `cancelled release`: cancellation teardown releases exchange-owned resident
  claims and drops buffered incoming claims safely

## Executable Harness Boundary

Local executable conformance coverage for this case set now lives in:

- `crates/tiforth-kernel/tests/exchange_slice.rs`
