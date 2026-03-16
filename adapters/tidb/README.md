# TiDB Adapter

This directory defines the TiDB-facing adapter boundary.

The adapter should eventually translate TiDB concepts into shared specs, data contracts, and runtime expectations without becoming the owner of semantics.

## TODOs

- define the minimal request and response surface needed by harnesses
- document TiDB-specific semantic mismatches found during inventory
- define which runtime concerns stay adapter-local
