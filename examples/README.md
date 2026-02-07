# TiForth examples

This directory contains small host-project examples for building and running TiForth.

- `bundled-arrow`: bundles Arrow via `FetchContent` and builds TiForth as a subproject.
- `system-arrow`: links against a system-installed Arrow via `find_package` and builds TiForth as a subproject.
- `arrow-common`: shared example source used by both builds.
