# TiFlash Adapter

This directory defines the TiFlash-facing adapter boundary.

The adapter should eventually connect TiFlash behavior to shared specs and contracts while keeping engine-specific execution details from leaking into the shared semantic layer.

Current checkpoint:

- `adapters/first-expression-slice.md` defines the minimal request and response surface for the first TiDB-versus-TiFlash differential slice

## TODOs

- extend the request and response surface beyond the first differential expression slice
- document TiFlash-specific semantic mismatches found during inventory
- define which runtime concerns stay adapter-local
