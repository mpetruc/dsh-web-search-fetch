"""dsh-web-research — the Hermes web-research pipeline ported for DeepSeek Harness.

This package is a dependency-free port of the Hermes-agent
`web-research` plugin (search + extract pipeline over local/in-process tools:
Hister cache, SearXNG, wreq TLS-fingerprint fetcher, Camofox browser fallback).
The DSH Host plugin (../index.js) drives it through `cli.py` as a
subprocess; there is no in-process Python embedding.
"""
