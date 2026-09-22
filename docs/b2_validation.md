# B2 local validation

Canonical checkout gate passed at C:\Projects\robottrust: clean main at 6cd821d, origin https://github.com/Electricpaper77/robottrust.git. Work is isolated on feat/b2-distributed-workers.

- Windows Python 3.12.14: full pytest suite 89 passed (48 unchanged B1 + 41 B2 cases). Separate B1 regression run: 48 passed.
- Built Linux image: full suite 89 passed. Small real Ray integration tests execute on both platforms, including remote exceptions and confirmed actor death. Third-party deprecation/future warnings remain visible.
- Ray 2.54.0; 8 logical CPUs. No 16-worker result claimed.
- Measured 1K, 10K and 100K episodes, seed 42, workers 1/2/4/8, three repetitions each: 12 configurations and 36 trials. All worker PIDs are distinct within each trial.
- Every trial's full canonical evaluation equals serial B1 exactly. Across measured trials: 1,332,000 expected and processed worker episode assignments; duplicates=0, missing=0, unknown=0; no failed benchmark tasks. Warmup is excluded.
- Independently regenerated datasets and checked dataset hashes, source fingerprint, all output metrics/decisions, raw-sample medians, throughput/speedup/efficiency formulas and JSON/CSV consistency.
- Existing B1 models, evaluator, metrics, policy, generator, API, evidence and tests are unchanged. B1 evidence SHA-256 remains valid.
- Docker robottrust:b2 builds successfully. Running container robottrust-b2 maps 127.0.0.1:8002 to 8000. GET /health returned 200 and status=ok; POST /evaluate returned the canonical 100-episode result. B1's port 8001 was not disturbed.
- GitHub Actions retains Python 3.11–3.13 coverage, installs Ray, runs all B1/B2 tests and existing evidence checks, with no benchmark matrix in CI.

## Observed limitations

Serial B1 is substantially faster than the Ray pipeline for this cheap computation. Canonical coordinator verification and reduction are serial bottlenecks. Best measured parallel speedup over one Ray worker is modest; adding workers is not linear scaling. Eight workers were slower than four at 1K and 100K. Startup overhead is recorded separately, and this is a warm-run local-machine experiment with three samples per configuration.

## Source provenance

Benchmark checkout SHA: `6cd821d0a3ac075a55989203cef9096be0231962` (dirty feature checkout before the final commit). Exact Python source plus pyproject SHA-256: `e8c803bc15220182e644ff7c83ff441d2d431c3875695c7171babac8b5942afe`. The recorded fingerprint matches the final benchmarked source. See b2_methodology.md for the hash construction and timing boundaries.
