# nats-bench
NATS bench jetstream

# Usage
```bash
usage: nats-bench [<flags>]

NATs client publisher


Flags:
      --[no-]help             Show context-sensitive help (also try --help-long and --help-man). ($NATS_BENCH_HELP)
      --mode="pub"            Mode pub or sub ($NATS_BENCH_MODE)
  -s, --server="nats://localhost:4222"  
                              NATs Endpoint ($NATS_BENCH_SERVER)
      --subject="NATS.BENCH"  NATs subject ($NATS_BENCH_SUBJECT)
      --stream="natsbenchstream"  
                              NATs subject ($NATS_BENCH_STREAM)
      --replicas=1            Number of replica ($NATS_BENCH_REPLICAS)
      --msgs=100              Number of message ($NATS_BENCH_MSGS)
      --sleep=10              Sleep time between interval in ms ($NATS_BENCH_SLEEP)
      --retry=10              Number of retry to NATS ($NATS_BENCH_RETRY)
      --retrywait=2           Number of retry wait to NATS in second ($NATS_BENCH_RETRYWAIT)
      --timeout=5             NATS timeout ($NATS_BENCH_TIMEOUT)
      --batch=100             Batch size ($NATS_BENCH_BATCH)
      --[no-]version          Show application version. ($NATS_BENCH_VERSION)
```
