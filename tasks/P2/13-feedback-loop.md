status: cancelled
agent: executor
completed_by: moonshotai/kimi-k2.6

## Cancellation Reason
The feedback/calibration loop (`result.calibrate()`) is cut from the roadmap because:
1. Temporal mismatch: burnt observes practice runs (post-development), but calibration requires production billing data
2. Telemetry burden: Building persistent calibration storage (local files or Delta tables) is a data platform, not a linter
3. Complexity vs value: With 2-3x accuracy targets, calibration provides marginal improvement at high implementation cost

Alternative: Update scaling coefficients via config file releases or web-sourced benchmarks.
