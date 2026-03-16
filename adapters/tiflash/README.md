# TiFlash Adapter

This directory defines the TiFlash-facing adapter boundary.

The adapter should eventually connect TiFlash behavior to shared specs and contracts while keeping engine-specific execution details from leaking into the shared semantic layer.

## TODOs

- define the minimal request and response surface needed by harnesses
- document TiFlash-specific semantic mismatches found during inventory
- define which runtime concerns stay adapter-local
