import { EventEmitter } from 'events'
import { createClient } from 'redis'
import { randomUUID } from 'crypto'
import ms from '@prsm/ms'
import { parseCronExpression, nextCronTime } from './parse.js'

/**
 * @typedef {Object} CronOptions
 * @property {{url?: string, host?: string, port?: number, password?: string}} [redis]
 * @property {string} [prefix]
 */

/**
 * @typedef {Object} JobOptions
 * @property {string|number} schedule - cron expression, @shortcut, or duration
 * @property {boolean} [exclusive] - prevent overlapping executions across instances
 * @property {string|number} [exclusiveTtl] - max hold time for exclusive lock (default 10m)
 */

/**
 * @typedef {Object} Job
 * @property {string} name
 * @property {'cron'|'interval'} type
 * @property {import('./parse.js').CronFields|null} fields
 * @property {number|null} interval
 * @property {boolean} exclusive
 * @property {number} exclusiveTtl
 * @property {function(): Promise<any>} handler
 */

const DEFAULT_EXCLUSIVE_TTL = 600000

export class Cron extends EventEmitter {
  /** @param {CronOptions} [options] */
  constructor(options = {}) {
    super()
    this._prefix = options.prefix ?? 'cron:'
    this._redis = createClient(options.redis ?? {})
    this._redis.on('error', () => {})
    this._readyPromise = this._redis.connect()
    this._instanceId = randomUUID()
    this._jobs = new Map()
    this._timers = new Map()
    this._active = new Set()
    this._running = false
    this._closed = false
  }

  /**
   * @param {string} name
   * @param {string|number|JobOptions} schedule
   * @param {function(): Promise<any>} handler
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

    const job = {
      name,
      type: parsed.type,
      fields: parsed.fields ?? null,
      interval: parsed.interval ?? null,
      exclusive: opts.exclusive ?? false,
      exclusiveTtl: ms(opts.exclusiveTtl ?? DEFAULT_EXCLUSIVE_TTL),
      handler,
    }

    this._jobs.set(name, job)
    if (this._running) this._scheduleNext(name, job)

    return this
  }

  /**
   * @param {string} name
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
    if (this._redis.isOpen) await this._redis.quit()
  }

  /** @returns {string[]} */
  get jobs() {
    return [...this._jobs.keys()]
  }

  /**
   * @param {string} name
   * @returns {Date|null}
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
    const lockKey = `${this._prefix}lock:${name}:${tickId}`
    const lockTtl = job.type === 'interval' ? Math.max(job.interval, 1000) : 60000

    let acquired
    try {
      acquired = await this._redis.set(lockKey, this._instanceId, { NX: true, PX: lockTtl })
    } catch {
      return
    }

    if (!acquired) return

    if (job.exclusive) {
      const runKey = `${this._prefix}running:${name}`
      let runLocked
      try {
        runLocked = await this._redis.set(runKey, this._instanceId, { NX: true, PX: job.exclusiveTtl })
      } catch {
        return
      }
      if (!runLocked) return
      try {
        await this._runHandler(name, tickId, job)
      } finally {
        await this._releaseLock(runKey).catch(() => {})
      }
    } else {
      await this._runHandler(name, tickId, job)
    }
  }

  /** @private */
  async _releaseLock(key) {
    await this._redis.eval(
      `if redis.call("get",KEYS[1]) == ARGV[1] then return redis.call("del",KEYS[1]) else return 0 end`,
      { keys: [key], arguments: [this._instanceId] },
    )
  }

  /** @private */
  async _runHandler(name, tickId, job) {
    try {
      const result = await job.handler()
      this.emit('fire', { name, tickId, result })
    } catch (error) {
      this.emit('error', { name, tickId, error })
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
