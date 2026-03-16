# @prsm/cron

redis-backed distributed cron scheduler with leader election and cron expression support.

## structure

```
src/
  index.js    - named exports
  cron.js     - Cron class
  parse.js    - cron expression parser + next-time calculator
tests/
  parse.test.js  - parser unit tests (no Redis)
  cron.test.js   - integration tests (requires Redis)
```

## dev

```
make up        # start redis via docker compose
make test      # run tests
make down      # stop redis
```

redis must be running on localhost:6379 for tests.

## key decisions

- plain javascript, ESM, no build step
- Cron class extends EventEmitter for lifecycle + events
- distributed lock via Redis SET NX PX - one instance fires per tick
- supports cron expressions (5-field), @shortcuts, and duration intervals via @prsm/ms
- uses `redis` npm package (node-redis), not ioredis
- types generated from JSDoc via `make types`
- timers are unref'd so they don't keep the process alive
- optional exclusive mode prevents overlapping executions across instances

## testing

tests use vitest. each test flushes redis in beforeEach. sequential execution.
parse tests are pure unit tests (no Redis). cron tests are integration tests.

## publishing

```
npm publish --access public
```

prepublishOnly generates types automatically.
