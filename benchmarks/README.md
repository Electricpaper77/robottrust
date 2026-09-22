# Benchmarks

B2 measures real local Ray evaluation wall time. See [generated summary](b2_summary.md), [raw samples and provenance](b2_scaling.json), [CSV](b2_scaling.csv), and [methodology](../docs/b2_methodology.md).

Synthetic episode inference-latency fields remain generated inputs, not service performance measurements. B2 timing uses perf_counter around actual evaluation paths and reports partition evaluator latency separately. The checked-out SHA plus exact source fingerprint identifies the benchmarked implementation, including uncommitted B2 source before its final commit.
