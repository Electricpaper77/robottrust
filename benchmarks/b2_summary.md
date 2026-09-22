# B2 measured local Ray scaling

Generated from real benchmark samples; medians of measured repetitions.

| Episodes | Workers | Seconds | Episodes/s | Speedup | Efficiency |
|---:|---:|---:|---:|---:|---:|
| 1,000 | 1 | 0.049349 | 20263.8 | 1.000x | 1.000 |
| 1,000 | 2 | 0.045180 | 22133.5 | 1.092x | 0.546 |
| 1,000 | 4 | 0.037369 | 26760.0 | 1.321x | 0.330 |
| 1,000 | 8 | 0.038772 | 25792.1 | 1.273x | 0.159 |
| 10,000 | 1 | 0.456945 | 21884.5 | 1.000x | 1.000 |
| 10,000 | 2 | 0.395379 | 25292.2 | 1.156x | 0.578 |
| 10,000 | 4 | 0.369556 | 27059.5 | 1.236x | 0.309 |
| 10,000 | 8 | 0.351333 | 28463.0 | 1.301x | 0.163 |
| 100,000 | 1 | 6.425811 | 15562.2 | 1.000x | 1.000 |
| 100,000 | 2 | 4.921223 | 20320.2 | 1.306x | 0.653 |
| 100,000 | 4 | 4.420621 | 22621.3 | 1.454x | 0.363 |
| 100,000 | 8 | 4.479566 | 22323.6 | 1.434x | 0.179 |

Speedup uses the matching Ray one-worker baseline, not serial B1. Serial B1 timings, startup, worker partition timings, exact results, integrity and every raw sample are in JSON.

These lightweight evaluations include serial integrity verification and canonical reduction; negative scaling is valid evidence. No sleeps or artificial workload are used.
