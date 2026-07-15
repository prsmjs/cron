import { EventEmitter } from 'events'
import { createClient } from 'redis'
import { randomUUID } from 'crypto'
import ms from '@prsm/ms'
import { mutex } from '@prsm/lock'
import { parseCronExpression, nextCronTime } from './parse.js'

/**
 * @typedef {Object} CronOptions
 * @property {{url?: string, host?: string, port?: number, password?: string}} [redis] - connection options for the node-redis client; provide either a url or discrete host/port/password fields (default connects to localhost:6379). All instances that should coordinate must point at the same Redis.
 * @property {string} [prefix] - key prefix for every lock and the pub/sub channel this scheduler uses (default "cron:"). Instances must share the same prefix to compete for the same ticks and to see each other's fire/error events.
 * @property {object} [tracer] - optional @prsm/trace tracer; when provided, each handler run is wrapped in a span named "cron.fire:<name>" (default none).
 */

/**
 * @typedef {Object} JobOptions
 * @property {string|number} schedule - when the job runs: a 5-field cron expression, an @shortcut (such as @daily), a duration string ("30s", "5m"), or an interval in milliseconds.
 * @property {boolean} [exclusive] - when true, prevents overlapping executions of this job across all instances and ticks (default false). While one run is in flight, every other instance and subsequent tick skips until it completes.
 * @property {string|number} [exclusiveTtl] - safety net for the exclusive run lock, as a duration string ("30m") or milliseconds (default 10m, "600000"). If the holder crashes mid-run, the lock auto-expires after this so the job is not blocked forever; set it longer than the handler can take.
 */

/**
 * @typedef {Object} Job
 * @property {string} name - unique name the job was registered under.
 * @property {'cron'|'interval'} type - "cron" for expression/@shortcut schedules, "interval" for duration or millisecond schedules.
 * @property {import('./parse.js').CronFields|null} fields - parsed cron fields for cron-type jobs, or null for interval jobs.
 * @property {number|null} interval - tick interval in milliseconds for interval jobs, or null for cron jobs.
 * @property {boolean} exclusive - whether overlapping executions across instances and ticks are prevented.
 * @property {number} exclusiveTtl - exclusive run lock TTL in milliseconds.
 * @property {function(): Promise<any>} handler - the function invoked when the job fires; its resolved value is delivered on the "fire" event.
 */

const DEFAULT_EXCLUSIVE_TTL = 600000

// node-redis only reads host/port from the nested socket object and silently
// ignores them at the top level, so lift the documented flat fields into place
function toClientOptions({ host, port, ...rest } = {}) {
  if (rest.url || (host === undefined && port === undefined)) return rest
  return { ...rest, socket: { host: host ?? '127.0.0.1', port: port ?? 6379, ...rest.socket } }
}

export class Cron extends EventEmitter {
  /** @param {CronOptions} [options] - scheduler configuration; all fields are optional and default to a localhost Redis with the "cron:" prefix. */
  constructor(options = {}) {
    super()
    this._tracer = options.tracer ?? null
    this._prefix = options.prefix ?? 'cron:'
    this._redis = createClient(toClientOptions(options.redis))
    this._redis.on('error', () => {})
    this._eventSub = this._redis.duplicate()
    this._eventSub.on('error', () => {})
    this._eventsChannel = `${this._prefix}events`
    this._instanceId = randomUUID()
    this._readyPromise = (async () => {
      await this._redis.connect()
      await this._eventSub.connect()
      await this._eventSub.subscribe(this._eventsChannel, (message) => this._onEventMessage(message))
    })()
    // annotated to keep @prsm/lock's internal mutex type from leaking into
    // the generated Cron declaration, which tsc cannot name portably (TS2742)
    /** @type {any} */
    this._lock = mutex({ redis: toClientOptions(options.redis), prefix: this._prefix })
    this._jobs = new Map()
    this._timers = new Map()
    this._active = new Set()
    this._running = false
    this._closed = false
  }

  /** @private */
  _onEventMessage(message) {
    let data
    try { data = JSON.parse(message) } catch { return }
    const { type, name, tickId, result, error, instanceId } = data
    if (type === 'fire') {
      this.emit('fire', { name, tickId, result, instanceId })
    } else if (type === 'error') {
      const err = error ? Object.assign(new Error(error.message ?? 'cron error'), error) : new Error('cron error')
      this.emit('error', { name, tickId, error: err, instanceId })
    }
  }

  /** @private */
  async _publishEvent(type, payload) {
    if (!this._redis.isOpen) return
    try {
      await this._redis.publish(this._eventsChannel, JSON.stringify({ type, ...payload, instanceId: this._instanceId }))
    } catch {}
  }

  /**
   * Register a job. Throws if a job with the same name already exists. Jobs may
   * be added before or after start(); adding after start begins scheduling
   * immediately.
   * @param {string} name - unique name for the job, used by remove(), run(), nextFireTime(), and in fire/error events.
   * @param {string|number|JobOptions} schedule - the schedule as a shorthand value (cron expression, @shortcut, duration string, or milliseconds), or a JobOptions object to also set exclusive/exclusiveTtl.
   * @param {function(): Promise<any>} handler - the function to run when the job fires; its resolved value is delivered on the "fire" event.
   * @returns {this}
   */
  add(name, schedule, handler) {
    if (this._closed) throw new Error('cron is stopped')
    if (this._jobs.has(name)) throw new Error(`job already exists: ${name}`)

    const opts = typeof schedule === 'string' || typeof schedule === 'number' ? { schedule } : schedule

    if (opts.schedule === undefined || opts.schedule === null) {
      throw new Error('schedule is required')
    }

    const parsed = parseScheduleValue(opts.schedule)

    const exclusiveTtl = ms(opts.exclusiveTtl ?? DEFAULT_EXCLUSIVE_TTL)
    if (!Number.isFinite(exclusiveTtl) || exclusiveTtl <= 0) {
      throw new Error('exclusiveTtl must be a positive duration')
    }

    const job = {
      name,
      type: parsed.type,
      fields: parsed.fields ?? null,
      interval: parsed.interval ?? null,
      exclusive: opts.exclusive ?? false,
      exclusiveTtl,
      handler,
    }

    this._jobs.set(name, job)
    if (this._running) this._scheduleNext(name, job)

    return this
  }

  /**
   * Stop and unregister a job. Clears its timer; in-flight runs are not aborted.
   * No-op if the job does not exist.
   * @param {string} name - name of the job to remove.
   * @returns {this}
   */
  remove(name) {
    const timer = this._timers.get(name)
    if (timer) clearTimeout(timer)
    this._timers.delete(name)
    this._jobs.delete(name)
    return this
  }

  async start() {
    if (this._closed) throw new Error('cron is stopped')
    await this._readyPromise
    if (this._closed) throw new Error('cron is stopped')
    this._running = true
    for (const [name, job] of this._jobs) {
      this._scheduleNext(name, job)
    }
  }

  async stop() {
    this._running = false
    this._closed = true
    for (const timer of this._timers.values()) clearTimeout(timer)
    this._timers.clear()
    await Promise.all(this._active)
    await this._readyPromise.catch(() => {})
    if (this._eventSub?.isOpen) await this._eventSub.unsubscribe().catch(() => {})
    if (this._eventSub?.isOpen) await this._eventSub.quit().catch(() => {})
    if (this._redis.isOpen) await this._redis.quit()
    await this._lock.close().catch(() => {})
  }

  /** @returns {string[]} */
  get jobs() {
    return [...this._jobs.keys()]
  }

  /**
   * Compute the next time the named job is scheduled to fire. This is the local
   * schedule time and does not account for whether another instance will win the
   * tick lock.
   * @param {string} name - name of the job.
   * @returns {Date|null} the next fire time, or null if the job does not exist or has no upcoming match within the search window.
   */
  nextFireTime(name) {
    const job = this._jobs.get(name)
    if (!job) return null
    if (job.type === 'interval') {
      const currentTick = Math.floor(Date.now() / job.interval)
      return new Date((currentTick + 1) * job.interval)
    }
    return nextCronTime(job.fields, Date.now())
  }

  /**
   * Run a job's handler immediately, regardless of its schedule. Emits 'fire'
   * or 'error' just like a scheduled run. For exclusive jobs the running lock
   * is respected: if the job is already running, this resolves without running.
   * @param {string} name - name of the job to run.
   * @returns {Promise<{ran: boolean, reason?: string}>} resolves with ran true once the handler completes; ran false (with a reason such as "already running" or "lock unavailable") when an exclusive job was skipped.
   */
  async run(name) {
    if (this._closed) throw new Error('cron is stopped')
    await this._readyPromise
    if (this._closed) throw new Error('cron is stopped')
    const job = this._jobs.get(name)
    if (!job) throw new Error(`job not found: ${name}`)

    const tickId = job.type === 'interval'
      ? Math.floor(Date.now() / job.interval)
      : Math.floor(Date.now() / 60000)

    if (job.exclusive) {
      const runKey = `running:${name}`
      let runResult
      try {
        runResult = await this._lock.acquire(runKey, { ttl: job.exclusiveTtl, id: this._instanceId })
      } catch {
        return { ran: false, reason: 'lock unavailable' }
      }
      if (!runResult.acquired) return { ran: false, reason: 'already running' }

      const promise = (async () => {
        try {
          await this._runHandler(name, tickId, job)
        } finally {
          await this._lock.release(runKey, this._instanceId).catch(() => {})
        }
      })()
      this._active.add(promise)
      promise.finally(() => this._active.delete(promise))
      await promise
      return { ran: true }
    }

    const promise = this._runHandler(name, tickId, job)
    this._active.add(promise)
    promise.finally(() => this._active.delete(promise))
    await promise
    return { ran: true }
  }

  /** @private */
  _scheduleNext(name, job) {
    if (!this._running || !this._jobs.has(name)) return

    let delay

    if (job.type === 'interval') {
      const now = Date.now()
      const currentTick = Math.floor(now / job.interval)
      const nextTickTime = (currentTick + 1) * job.interval
      delay = nextTickTime - now
    } else {
      const next = nextCronTime(job.fields, Date.now())
      if (!next) return
      delay = next.getTime() - Date.now()
    }

    delay = Math.max(delay, 0)

    const timer = setTimeout(() => this._tick(name, job), delay)
    timer.unref()
    this._timers.set(name, timer)
  }

  /** @private */
  _tick(name, job) {
    if (!this._running || !this._jobs.has(name)) return

    const tickId = job.type === 'interval' ? Math.floor(Date.now() / job.interval) : Math.floor(Date.now() / 60000)

    const promise = this._executeTick(name, job, tickId)
    this._active.add(promise)
    promise.finally(() => {
      this._active.delete(promise)
      this._scheduleNext(name, job)
    })
  }

  /** @private */
  async _executeTick(name, job, tickId) {
    const lockKey = `lock:${name}:${tickId}`
    const lockTtl = job.type === 'interval' ? Math.max(job.interval, 1000) : 60000

    let result
    try {
      result = await this._lock.acquire(lockKey, { ttl: lockTtl, id: this._instanceId })
    } catch {
      return
    }

    if (!result.acquired) return

    if (job.exclusive) {
      const runKey = `running:${name}`
      let runResult
      try {
        runResult = await this._lock.acquire(runKey, { ttl: job.exclusiveTtl, id: this._instanceId })
      } catch {
        return
      }
      if (!runResult.acquired) return
      try {
        await this._runHandler(name, tickId, job)
      } finally {
        await this._lock.release(runKey, this._instanceId).catch(() => {})
      }
    } else {
      await this._runHandler(name, tickId, job)
    }
  }

  /** @private */
  async _runHandler(name, tickId, job) {
    let result
    let handlerError
    const exec = async () => {
      result = await job.handler()
    }
    try {
      if (this._tracer) {
        await this._tracer.span(`cron.fire:${name}`, { 'cron.name': name, 'cron.tickId': tickId }, exec)
      } else {
        await exec()
      }
    } catch (error) {
      handlerError = error
    }
    if (handlerError) {
      await this._publishEvent('error', {
        name,
        tickId,
        error: { message: handlerError?.message, name: handlerError?.name, stack: handlerError?.stack },
      })
    } else {
      await this._publishEvent('fire', { name, tickId, result })
    }
  }
}

function parseScheduleValue(schedule) {
  if (typeof schedule === 'number') {
    if (!Number.isFinite(schedule) || schedule <= 0) {
      throw new Error('interval must be a positive number')
    }
    return { type: 'interval', interval: schedule }
  }

  const trimmed = schedule.trim()

  if (trimmed.startsWith('@') || trimmed.split(/\s+/).length === 5) {
    return { type: 'cron', fields: parseCronExpression(trimmed) }
  }

  const interval = ms(trimmed)
  if (!Number.isFinite(interval) || interval <= 0) {
    throw new Error('schedule must be a valid cron expression, @shortcut, or positive duration')
  }

  return { type: 'interval', interval }
}
