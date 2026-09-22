# B1 local validation

Validated on Windows with Python 3.12.14.

- Repository root: C:\Projects\robottrust; branch: main.
- Dependencies installed successfully; pip check reported no broken requirements.
- Full suite: 48 passed, with two upstream TestClient deprecation warnings.
- System pytest temporary directory was inaccessible. Tests passed using a fresh directory under ignored work/ via --basetemp.
- Generated and schema-validated 100 episodes with seed 42; all five scenarios represented.
- Replayed and evaluated all 100 records: success 0.92, collision incidence 0.04, safety violations 0.0, mean latency 232.7696 ms, nearest-rank p95 427.667 ms, mean dropout 0.08566611, failed episodes 8. Decision: PASS.
- All three evidence artifacts exist; their SHA-256 hashes match checksums.json. JSON and JSONL use explicit LF endings to preserve hashes across Git checkouts.
- Live Uvicorn started successfully; /health and POST /evaluate returned HTTP 200 with the expected results. The validation server was then stopped.
- Git diff and candidate files reviewed; caches, virtual environment, wheel/build output, and temporary files are ignored. No secrets found in the implementation.
- GitHub Actions workflow inspected: installs dependencies, runs the full pytest suite on Python 3.11–3.13, generates evidence and verifies checksums. REMOTE CI: PENDING — no GitHub remote configured (git remote -v returned no entries).
- Docker engine: PASS (29.1.3). Existing image robottrust:b1 inspected successfully: sha256:dcc0c124a40bc3fdd693f447a1a3392f109a535c4f7fbad5308897864b99dd6f. Docker build: PASS; existing image reused without rebuilding. Container robottrust-b1 runs with host port 8001 mapped to container port 8000. Live GET /health and GET /openapi.json returned HTTP 200; health returned status=ok. POST /evaluate over all 100 records returned HTTP 200 and exactly matched local evaluation. The previously reported engine blocker is resolved.
- Final canonical validation: all 48 tests pass; seed-42 generation reproduces the same 100 schema-valid records. Metrics and policy decision were independently recalculated from those records; replay, JSON artifacts and SHA-256 checksums match. All local B1 gates pass. B2 has not started.
