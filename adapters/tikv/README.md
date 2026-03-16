# TiKV Adapter

This directory defines the TiKV-facing adapter boundary.

The adapter should eventually translate TiKV expression and operator behavior into shared specs and contracts without turning adapter code into the semantic source of truth.

## TODOs

- define the minimal request and response surface needed by harnesses
- document TiKV-specific semantic mismatches found during inventory
- define which runtime concerns stay adapter-local
