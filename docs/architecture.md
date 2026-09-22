# B1 evaluation contract

Synthetic Robot Episode → Pydantic Episode validation → metrics → configurable release policy → JSONL and JSON evidence.

`generator` uses a local seeded PRNG, stable IDs and a fixed UTC timestamp origin. Scenarios cycle through all five supported values. Emergency-stop success means a safe stop, not continued navigation. Generated inputs are synthetic, not simulator or robot measurements.

`metrics` rejects empty batches and duplicate episode IDs. Collision rate counts episodes with one or more collisions, not total collisions per episode. Rates are fractions in [0, 1]. p95 uses nearest rank: sorted latency at ceil(0.95 * n), with one-based indexing. Failures are episodes with success=false. Safety, collisions and task success remain independent observations.

`policy` applies strict inequalities with BLOCK precedence. All configured thresholds and applicable failure reasons are saved with the result. Policy versions on episodes identify their source policy; a batch may contain multiple versions and reports aggregate metrics across the entire supplied batch.

`evidence` evaluates the supplied records before writing them and hashes exact file bytes. checksums.json covers episodes.jsonl, evaluation.json and decision.json; a manifest cannot hash itself. Replay validates each line and reports the line number on failure. Checksum verification detects changes, not malicious re-signing. Writes replace existing artifacts; use a unique output directory per run and a single writer. Multi-file transactional publication is outside B1.

The API evaluates supplied records without writing files. GET endpoints expose the last completed evaluation in the current process and return 404 before an evaluation. Restart clears this state; multiple workers do not share it. Use the evaluator CLI to persist evidence. API validation errors return 422.

Future adapters may translate ROS 2 or simulator events into Episode records, schedule evaluation remotely, or upload evidence. B1 implements none of those integrations or distributed infrastructure.
