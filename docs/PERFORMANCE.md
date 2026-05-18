# Performance notes

## Reproduce throughput table

```bash
cmake --build build --target bench
./benchmarks/run_sweep.sh
```

Results: `benchmarks/results.csv` (current). Historical baselines: `results_baseline.csv` (serial shuffle on thread 0), `results_map_tree.csv` (map tree merge).

## Shuffle evolution

1. **Serial** — thread 0 inserts all pairs into one `std::map`.
2. **Map tree** — per-thread local maps, tree merge of maps.
3. **Sorted-run merge (current)** — per-thread sort, then parallel `std::merge` tree on sorted pair runs, then linear grouping into reduce buckets. Avoids map inserts during shuffle.

## Profiling (Linux)

```bash
perf stat -d ./build/bench --threads 8 --inputs 500000
perf record -g ./build/bench --threads 8 --inputs 500000
perf report --stdio | head -80
```

At high thread counts, cost shifts to merge barriers and memory traffic; map/reduce still use atomic work distribution.
