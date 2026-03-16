<p align="center">
  <img src=".github/logo.svg" width="80" height="80" alt="cron logo">
</p>

<h1 align="center">@prsm/cron</h1>

Redis-backed distributed cron scheduler. Run jobs on a schedule across multiple instances - only one fires per tick.

## Installation

```bash
npm install @prsm/cron
```

## Quick Start

```js
import { Cron } from '@prsm/cron'

const cron = new Cron()

cron.add('cleanup', '*/5 * * * *', async () => {
  return await db.query('DELETE FROM temp WHERE created_at < NOW() - INTERVAL 1 HOUR')
})

cron.on('fire', ({ name, result }) => {
  console.log('Fired:', name, result)
})

cron.on('error', ({ name, error }) => {
  console.error('Failed:', name, error.message)
})

await cron.start()
```

## Schedule Formats

Three ways to define when a job runs:

```js
cron.add('reports', '0 2 * * *', handler) // cron expression
cron.add('heartbeat', '30s', handler) // duration string
cron.add('poll', 5000, handler) // milliseconds
```

### Cron Expressions

Standard 5-field format: `minute hour day-of-month month day-of-week`

| Field        | Range | Allowed                             |
| ------------ | ----- | ----------------------------------- |
| Minute       | 0-59  | `*`, `,`, `-`, `/`                  |
| Hour         | 0-23  | `*`, `,`, `-`, `/`                  |
| Day of month | 1-31  | `*`, `,`, `-`, `/`                  |
| Month        | 1-12  | `*`, `,`, `-`, `/`, names (jan-dec) |
| Day of week  | 0-7   | `*`, `,`, `-`, `/`, names (sun-sat) |

Day 0 and 7 both mean Sunday. When both day-of-month and day-of-week are specified (not `*`), either matching triggers the job (OR logic, per standard cron).

### Shortcuts

| Shortcut    | Equivalent  |
| ----------- | ----------- |
| `@yearly`   | `0 0 1 1 *` |
| `@annually` | `0 0 1 1 *` |
| `@monthly`  | `0 0 1 * *` |
| `@weekly`   | `0 0 * * 0` |
| `@daily`    | `0 0 * * *` |
| `@midnight` | `0 0 * * *` |
| `@hourly`   | `0 * * * *` |

### Duration Strings

Parsed by [@prsm/ms](https://github.com/nvms/ms): `'100ms'`, `'5s'`, `'1m'`, `'1h'`.

## Options

```js
const cron = new Cron({
  redis: {
    host: 'localhost',
    port: 6379,
    password: 'secret',
  },
  prefix: 'myapp:cron:', // default: 'cron:'
})
```

## Exclusive Mode

By default, if a handler runs longer than the interval, the next tick can start a new execution on another instance. Enable exclusive mode to prevent overlapping:

```js
cron.add(
  'reports',
  {
    schedule: '0 2 * * *',
    exclusive: true,
    exclusiveTtl: '30m', // max lock hold time (default 10m)
  },
  async () => {
    await generateDailyReport()
  }
)
```

While one instance is running the handler, all other instances (and subsequent ticks on the same instance) skip until it completes. The TTL is a safety net - if the instance crashes, the lock auto-releases after `exclusiveTtl`.

## Job Management

```js
cron.add('a', '30s', handler) // register
cron.add('b', '1m', handler) // chainable - returns this
cron.remove('a') // stop and unregister
cron.jobs // ['b']
cron.nextFireTime('b') // Date or null
```

Jobs can be added before or after `start()`. Adding after start begins scheduling immediately.

## Events

```js
cron.on('fire', ({ name, tickId, result }) => {})
cron.on('error', ({ name, tickId, error }) => {})
```

## How It Works

Each instance runs its own timers. When a timer fires, it attempts a Redis `SET key NX PX ttl` for that specific tick window. Only one instance succeeds - the rest see the key already exists and skip. No leader election protocol, no consensus - just an atomic Redis operation.

For interval jobs, ticks are epoch-aligned: `tickId = Math.floor(Date.now() / interval)`. All instances compute the same tick ID independently, so they compete for the same lock regardless of when they started.

For cron jobs, ticks are minute-aligned: `tickId = Math.floor(Date.now() / 60000)`. The cron parser computes the next matching minute and sets a timeout for it.

## Scheduled Queue Processing with [queue](https://github.com/nvms/queue)

Push work into a queue on a schedule:

```js
import { Cron } from '@prsm/cron'
import Queue from '@prsm/queue'

const cron = new Cron()
const queue = new Queue({ concurrency: 5 })

queue.process(async (payload) => {
  return await syncTenant(payload)
})

cron.add('sync-all-tenants', '0 */6 * * *', async () => {
  const tenants = await db.query('SELECT id FROM tenants WHERE active = true')
  for (const t of tenants) {
    await queue.group(t.id).push({ tenantId: t.id })
  }
})

await queue.ready()
await cron.start()
```

Every 6 hours, one instance enqueues sync tasks for all tenants. The queue distributes the actual work across all instances with per-tenant concurrency control.

## Real-Time Status with [mesh](https://github.com/nvms/mesh)

Broadcast scheduled job results to connected clients:

```js
import { Cron } from '@prsm/cron'
import { MeshServer } from '@mesh-kit/server'

const mesh = new MeshServer({ redis: { host: 'localhost', port: 6379 } })
const cron = new Cron()

cron.add('leaderboard', '*/5 * * * *', async () => {
  return await computeLeaderboard()
})

cron.on('fire', ({ name, result }) => {
  mesh.broadcastRoom('dashboard', `cron:${name}`, result)
})

await mesh.listen(8080)
await cron.start()
```

## Cleanup

```js
await cron.stop()
```

Clears all timers, waits for in-flight handlers to complete, then disconnects Redis.

## Horizontal Scaling

All lock state lives in Redis. Deploy as many instances as you want - Redis `SET NX` guarantees exactly-once execution per tick. No configuration changes needed. Lock keys auto-expire, so crashed instances don't leave stale locks.

## License

MIT
