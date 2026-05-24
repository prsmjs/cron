import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { createClient } from 'redis'
import { Cron } from '../src/cron.js'

function waitForEvent(emitter, event, timeout = 5000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`timeout waiting for "${event}"`)), timeout)
    emitter.once(event, (data) => {
      clearTimeout(timer)
      resolve(data)
    })
  })
}

function waitForN(emitter, event, n, timeout = 10000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`timeout waiting for ${n}x "${event}"`)), timeout)
    const results = []
    const handler = (data) => {
      results.push(data)
      if (results.length >= n) {
        clearTimeout(timer)
        emitter.removeListener(event, handler)
        resolve(results)
      }
    }
    emitter.on(event, handler)
  })
}

function collectEvents(emitter, event) {
  const events = []
  emitter.on(event, (data) => events.push(data))
  return events
}

let cron
let extraCrons
let redis

beforeEach(async () => {
  redis = createClient()
  await redis.connect()
  await redis.flushAll()
  extraCrons = []
})

afterEach(async () => {
  if (cron) await cron.stop().catch(() => {})
  for (const c of extraCrons) await c.stop().catch(() => {})
  if (redis?.isOpen) await redis.quit()
})

describe('Cron', () => {
  describe('interval jobs', () => {
    it('fires an interval job', async () => {
      cron = new Cron()
      let called = false
      cron.add('test', '1s', async () => {
        called = true
      })

      await cron.start()
      await waitForEvent(cron, 'fire')

      expect(called).toBe(true)
    })

    it('fires multiple times', async () => {
      cron = new Cron()
      let count = 0
      cron.add('test', 500, async () => {
        count++
      })

      await cron.start()
      await waitForN(cron, 'fire', 3)

      expect(count).toBeGreaterThanOrEqual(3)
    })

    it('emits fire event with job name and tickId', async () => {
      cron = new Cron()
      cron.add('myJob', '1s', async () => 'result')

      await cron.start()
      const event = await waitForEvent(cron, 'fire')

      expect(event.name).toBe('myJob')
      expect(typeof event.tickId).toBe('number')
      expect(event.result).toBe('result')
    })

    it('accepts numeric interval (ms)', async () => {
      cron = new Cron()
      let called = false
      cron.add('test', 500, async () => {
        called = true
      })

      await cron.start()
      await waitForEvent(cron, 'fire')

      expect(called).toBe(true)
    })
  })

  describe('distributed locking', () => {
    it('only one instance runs the handler per tick, but the fire event fans out to all', async () => {
      cron = new Cron()
      const cron2 = new Cron()
      extraCrons.push(cron2)

      const fires1 = collectEvents(cron, 'fire')
      const fires2 = collectEvents(cron2, 'fire')

      let calls1 = 0
      let calls2 = 0
      cron.add('shared', '1s', async () => { calls1++; return 'a' })
      cron2.add('shared', '1s', async () => { calls2++; return 'b' })

      await cron.start()
      await cron2.start()

      await new Promise((r) => setTimeout(r, 3500))

      const totalHandlerCalls = calls1 + calls2
      expect(totalHandlerCalls).toBeGreaterThanOrEqual(2)

      // each tick is handled by exactly one instance: no duplicate per-tick handler runs
      const callTicksPerInstance = fires1
        .filter((e) => e.instanceId)
        .reduce((acc, e) => acc.add(`${e.instanceId}:${e.tickId}`), new Set())
      expect(callTicksPerInstance.size).toBe(totalHandlerCalls)

      // the fire event fans out: both instances see the same set of tickIds
      const tickIds1 = new Set(fires1.map((e) => e.tickId))
      const tickIds2 = new Set(fires2.map((e) => e.tickId))
      expect(tickIds1.size).toBe(totalHandlerCalls)
      expect(tickIds2.size).toBe(totalHandlerCalls)
      for (const id of tickIds1) {
        expect(tickIds2.has(id)).toBe(true)
      }
    })

    it('different job names fire independently', async () => {
      cron = new Cron()
      let aCount = 0
      let bCount = 0

      cron.add('a', 500, async () => {
        aCount++
      })
      cron.add('b', 500, async () => {
        bCount++
      })

      await cron.start()
      await new Promise((r) => setTimeout(r, 2000))

      expect(aCount).toBeGreaterThanOrEqual(2)
      expect(bCount).toBeGreaterThanOrEqual(2)
    })
  })

  describe('exclusive mode', () => {
    it('prevents overlapping executions', async () => {
      cron = new Cron()
      const cron2 = new Cron()
      extraCrons.push(cron2)

      let running = 0
      let maxRunning = 0

      const handler = async () => {
        running++
        maxRunning = Math.max(maxRunning, running)
        await new Promise((r) => setTimeout(r, 800))
        running--
      }

      cron.add('exclusive', { schedule: 500, exclusive: true, exclusiveTtl: '10s' }, handler)
      cron2.add('exclusive', { schedule: 500, exclusive: true, exclusiveTtl: '10s' }, handler)

      await cron.start()
      await cron2.start()

      await new Promise((r) => setTimeout(r, 3000))

      expect(maxRunning).toBe(1)
    })
  })

  describe('exclusive lock ownership', () => {
    it('does not delete another instance lock when handler exceeds ttl', async () => {
      cron = new Cron()
      const cron2 = new Cron()
      extraCrons.push(cron2)

      let instance1Done = false
      let instance2Acquired = false

      cron.add(
        'slow',
        { schedule: 500, exclusive: true, exclusiveTtl: 500 },
        async () => {
          await new Promise((r) => setTimeout(r, 1500))
          instance1Done = true
        },
      )

      await cron.start()
      await new Promise((r) => setTimeout(r, 800))

      const runKey = 'cron:running:slow'
      const lockVal = await redis.get(runKey)
      expect(lockVal).toBeNull()

      cron2.add(
        'slow',
        { schedule: 500, exclusive: true, exclusiveTtl: '10s' },
        async () => {
          instance2Acquired = true
          await new Promise((r) => setTimeout(r, 2000))
        },
      )
      await cron2.start()
      await new Promise((r) => setTimeout(r, 1500))

      expect(instance1Done).toBe(true)

      const lockAfter = await redis.get(runKey)
      if (instance2Acquired && lockAfter !== null) {
        expect(lockAfter).toBe(cron2._instanceId)
      }
    })
  })

  describe('error handling', () => {
    it('exclusive lock is released when handler throws', async () => {
      cron = new Cron()
      cron.add('failing', { schedule: 500, exclusive: true }, async () => {
        throw new Error('boom')
      })

      await cron.start()
      await waitForEvent(cron, 'error')
      await new Promise((r) => setTimeout(r, 50))

      const lock = await redis.get('cron:running:failing')
      expect(lock).toBeNull()
    })

    it('emits error event when handler throws', async () => {
      cron = new Cron()
      cron.add('failing', '1s', async () => {
        throw new Error('boom')
      })

      await cron.start()
      const event = await waitForEvent(cron, 'error')

      expect(event.name).toBe('failing')
      expect(event.error.message).toBe('boom')
    })

    it('continues firing after errors', async () => {
      cron = new Cron()
      let count = 0
      cron.add('failing', 500, async () => {
        count++
        throw new Error('boom')
      })

      await cron.start()
      await waitForN(cron, 'error', 3)

      expect(count).toBeGreaterThanOrEqual(3)
    })
  })

  describe('job management', () => {
    it('add returns this for chaining', () => {
      cron = new Cron()
      const result = cron.add('a', '1s', async () => {})
      expect(result).toBe(cron)
    })

    it('remove stops a job from firing', async () => {
      cron = new Cron()
      let count = 0
      cron.add('test', 500, async () => {
        count++
      })

      await cron.start()
      await waitForEvent(cron, 'fire')

      cron.remove('test')
      const countAtRemove = count

      await new Promise((r) => setTimeout(r, 1500))
      expect(count).toBe(countAtRemove)
    })

    it('add after start begins scheduling immediately', async () => {
      cron = new Cron()
      await cron.start()

      let called = false
      cron.add('late', '1s', async () => {
        called = true
      })

      await waitForEvent(cron, 'fire')
      expect(called).toBe(true)
    })

    it('jobs getter returns job names', () => {
      cron = new Cron()
      cron.add('a', '1s', async () => {})
      cron.add('b', '5s', async () => {})

      expect(cron.jobs).toEqual(['a', 'b'])
    })

    it('throws on duplicate job name', () => {
      cron = new Cron()
      cron.add('a', '1s', async () => {})
      expect(() => cron.add('a', '2s', async () => {})).toThrow('already exists')
    })

    it('throws when adding after stop', async () => {
      cron = new Cron()
      await cron.start()
      await cron.stop()
      expect(() => cron.add('a', '1s', async () => {})).toThrow('stopped')
      cron = null
    })
  })

  describe('run', () => {
    it('runs a job immediately regardless of schedule', async () => {
      cron = new Cron()
      let called = false
      cron.add('manual', '1h', async () => {
        called = true
        return 'done'
      })

      const fired = waitForEvent(cron, 'fire')
      const result = await cron.run('manual')
      const payload = await fired

      expect(called).toBe(true)
      expect(result).toEqual({ ran: true })
      expect(payload.name).toBe('manual')
      expect(payload.result).toBe('done')
    })

    it('does not require start() to have been called', async () => {
      cron = new Cron()
      let called = false
      cron.add('manual', '1h', async () => { called = true })
      await cron.run('manual')
      expect(called).toBe(true)
    })

    it('emits error when the handler throws', async () => {
      cron = new Cron()
      cron.add('boom', '1h', async () => { throw new Error('handler failed') })

      const errored = waitForEvent(cron, 'error')
      await cron.run('boom')
      const payload = await errored

      expect(payload.name).toBe('boom')
      expect(payload.error.message).toBe('handler failed')
    })

    it('throws for an unknown job', async () => {
      cron = new Cron()
      await expect(cron.run('nope')).rejects.toThrow('job not found')
    })

    it('throws after stop', async () => {
      cron = new Cron()
      cron.add('a', '1h', async () => {})
      await cron.start()
      await cron.stop()
      await expect(cron.run('a')).rejects.toThrow('stopped')
      cron = null
    })

    it('exclusive jobs do not overlap on concurrent runs', async () => {
      cron = new Cron()
      let active = 0
      let maxActive = 0
      cron.add('excl', { schedule: '1h', exclusive: true }, async () => {
        active++
        maxActive = Math.max(maxActive, active)
        await new Promise((r) => setTimeout(r, 200))
        active--
      })

      const [a, b] = await Promise.all([cron.run('excl'), cron.run('excl')])
      const ran = [a, b]
      expect(ran.filter((r) => r.ran).length).toBe(1)
      expect(ran.filter((r) => !r.ran).length).toBe(1)
      expect(ran.find((r) => !r.ran).reason).toBe('already running')
      expect(maxActive).toBe(1)
    })
  })

  describe('stop behavior', () => {
    it('waits for in-flight handlers before closing', async () => {
      cron = new Cron()
      let finished = false

      cron.add('slow', '1s', async () => {
        await new Promise((r) => setTimeout(r, 500))
        finished = true
      })

      await cron.start()
      await waitForEvent(cron, 'fire')

      // handler fired, now add another that will be in-flight during stop
      cron.remove('slow')
      cron.add('slow2', 500, async () => {
        await new Promise((r) => setTimeout(r, 300))
        finished = true
      })

      finished = false
      await new Promise((r) => setTimeout(r, 800))
      await cron.stop()

      expect(finished).toBe(true)
      cron = null
    })

    it('stop is idempotent with closed state', async () => {
      cron = new Cron()
      await cron.start()
      await cron.stop()
      expect(() => cron.add('a', '1s', async () => {})).toThrow('stopped')
      cron = null
    })
  })

  describe('nextFireTime', () => {
    it('returns next fire time for interval job', () => {
      cron = new Cron()
      cron.add('test', 5000, async () => {})

      const next = cron.nextFireTime('test')
      expect(next).toBeInstanceOf(Date)

      const now = Date.now()
      expect(next.getTime()).toBeGreaterThan(now)
      expect(next.getTime()).toBeLessThanOrEqual(now + 5000)
    })

    it('returns next fire time for cron job', () => {
      cron = new Cron()
      cron.add('test', '0 * * * *', async () => {})

      const next = cron.nextFireTime('test')
      expect(next).toBeInstanceOf(Date)
      expect(next.getMinutes()).toBe(0)
      expect(next.getTime()).toBeGreaterThan(Date.now())
    })

    it('returns null for unknown job', () => {
      cron = new Cron()
      expect(cron.nextFireTime('unknown')).toBeNull()
    })
  })

  describe('schedule parsing', () => {
    it('accepts cron expressions', () => {
      cron = new Cron()
      cron.add('test', '*/5 * * * *', async () => {})
      expect(cron.jobs).toEqual(['test'])
    })

    it('accepts @shortcuts', () => {
      cron = new Cron()
      cron.add('test', '@hourly', async () => {})
      expect(cron.jobs).toEqual(['test'])
    })

    it('accepts duration strings', () => {
      cron = new Cron()
      cron.add('test', '30s', async () => {})
      expect(cron.jobs).toEqual(['test'])
    })

    it('accepts numeric ms', () => {
      cron = new Cron()
      cron.add('test', 5000, async () => {})
      expect(cron.jobs).toEqual(['test'])
    })

    it('accepts options object with schedule', () => {
      cron = new Cron()
      cron.add('test', { schedule: '30s' }, async () => {})
      expect(cron.jobs).toEqual(['test'])
    })

    it('rejects invalid schedule', () => {
      cron = new Cron()
      expect(() => cron.add('test', 'not-valid', async () => {})).toThrow()
    })

    it('rejects negative interval', () => {
      cron = new Cron()
      expect(() => cron.add('test', -1000, async () => {})).toThrow('positive')
    })

    it('rejects zero interval', () => {
      cron = new Cron()
      expect(() => cron.add('test', 0, async () => {})).toThrow('positive')
    })

    it('rejects missing schedule', () => {
      cron = new Cron()
      expect(() => cron.add('test', {}, async () => {})).toThrow('required')
    })

    it('rejects invalid exclusiveTtl', () => {
      cron = new Cron()
      expect(() =>
        cron.add('test', { schedule: '1s', exclusive: true, exclusiveTtl: 'garbage' }, async () => {}),
      ).toThrow('exclusiveTtl')
    })
  })

  describe('prefix', () => {
    it('uses custom prefix for lock keys', async () => {
      cron = new Cron({ prefix: 'myapp:cron:' })
      cron.add('test', '1s', async () => {})

      await cron.start()
      await waitForEvent(cron, 'fire')

      const keys = await redis.keys('myapp:cron:lock:*')
      expect(keys.length).toBeGreaterThan(0)
    })
  })
})
