const MONTHS = { jan: 1, feb: 2, mar: 3, apr: 4, may: 5, jun: 6, jul: 7, aug: 8, sep: 9, oct: 10, nov: 11, dec: 12 }
const DAYS = { sun: 0, mon: 1, tue: 2, wed: 3, thu: 4, fri: 5, sat: 6 }

const SHORTCUTS = {
  '@yearly': '0 0 1 1 *',
  '@annually': '0 0 1 1 *',
  '@monthly': '0 0 1 * *',
  '@weekly': '0 0 * * 0',
  '@daily': '0 0 * * *',
  '@midnight': '0 0 * * *',
  '@hourly': '0 * * * *',
}

function resolveValue(value, names) {
  if (names) {
    const mapped = names[value.toLowerCase()]
    if (mapped !== undefined) return mapped
  }
  const n = parseInt(value, 10)
  if (isNaN(n)) throw new Error(`invalid cron value: ${value}`)
  return n
}

function parseField(field, min, max, names) {
  const values = new Set()

  for (const part of field.split(',')) {
    const [rangeStr, stepStr] = part.split('/')
    const step = stepStr !== undefined ? parseInt(stepStr, 10) : 1

    if (isNaN(step) || step < 1) throw new Error(`invalid step in cron field: ${part}`)

    let start, end

    if (rangeStr === '*') {
      start = min
      end = max
    } else if (rangeStr.includes('-')) {
      const [a, b] = rangeStr.split('-')
      start = resolveValue(a, names)
      end = resolveValue(b, names)
    } else {
      start = resolveValue(rangeStr, names)
      end = stepStr !== undefined ? max : start
    }

    if (start < min || start > max) throw new Error(`cron value out of range: ${start} (${min}-${max})`)
    if (end < min || end > max) throw new Error(`cron value out of range: ${end} (${min}-${max})`)
    if (end < start) throw new Error(`invalid cron range: ${start}-${end}`)

    for (let i = start; i <= end; i += step) {
      values.add(i)
    }
  }

  return [...values].sort((a, b) => a - b)
}

/**
 * @typedef {Object} CronFields
 * @property {number[]} minute - sorted minutes (0-59) the job may fire on, expanded from the minute field.
 * @property {number[]} hour - sorted hours (0-23) the job may fire on, expanded from the hour field.
 * @property {number[]} dom - sorted days of the month (1-31) the job may fire on, expanded from the day-of-month field.
 * @property {number[]} month - sorted months (1-12) the job may fire in, expanded from the month field.
 * @property {number[]} dow - sorted days of the week (0-6, Sunday is 0) the job may fire on; the cron value 7 is normalized to 0.
 * @property {boolean} domWild - whether the day-of-month field was "*". When both domWild and dowWild are false, day-of-month and day-of-week are combined with OR logic, per standard cron.
 * @property {boolean} dowWild - whether the day-of-week field was "*".
 */

/**
 * Parse a 5-field cron expression or @shortcut into expanded, sorted field sets.
 * @param {string} expression - a 5-field cron expression ("minute hour day-of-month month day-of-week") or an @shortcut such as @daily. Throws if it does not resolve to exactly 5 fields or contains an out-of-range value.
 * @returns {CronFields}
 */
export function parseCronExpression(expression) {
  const resolved = SHORTCUTS[expression.trim().toLowerCase()] ?? expression
  const parts = resolved.trim().split(/\s+/)
  if (parts.length !== 5) throw new Error('cron expression must have exactly 5 fields')

  const dow = parseField(parts[4], 0, 7, DAYS)

  return {
    minute: parseField(parts[0], 0, 59),
    hour: parseField(parts[1], 0, 23),
    dom: parseField(parts[2], 1, 31),
    month: parseField(parts[3], 1, 12, MONTHS),
    dow: [...new Set(dow.map((v) => (v === 7 ? 0 : v)))].sort((a, b) => a - b),
    domWild: parts[2] === '*',
    dowWild: parts[4] === '*',
  }
}

function matchesDay(fields, dom, dow) {
  if (fields.domWild && fields.dowWild) return true
  if (fields.domWild) return fields.dow.includes(dow)
  if (fields.dowWild) return fields.dom.includes(dom)
  return fields.dom.includes(dom) || fields.dow.includes(dow)
}

/**
 * Find the next minute-aligned time at or after a given point that matches the
 * parsed cron fields.
 * @param {CronFields} fields - parsed cron fields to match against.
 * @param {number|Date} after - timestamp in milliseconds or a Date to search from; the result is strictly after the start of the containing minute.
 * @returns {Date|null} the next matching time, or null if no match falls within the 4-year search window.
 */
export function nextCronTime(fields, after) {
  const ts = typeof after === 'number' ? after : after.getTime()
  const d = new Date(ts)
  d.setSeconds(0, 0)
  d.setMinutes(d.getMinutes() + 1)

  const limit = new Date(d)
  limit.setFullYear(limit.getFullYear() + 4)

  while (d < limit) {
    if (!fields.month.includes(d.getMonth() + 1)) {
      d.setMonth(d.getMonth() + 1, 1)
      d.setHours(0, 0, 0, 0)
      continue
    }

    if (!matchesDay(fields, d.getDate(), d.getDay())) {
      d.setDate(d.getDate() + 1)
      d.setHours(0, 0, 0, 0)
      continue
    }

    if (!fields.hour.includes(d.getHours())) {
      d.setHours(d.getHours() + 1, 0, 0, 0)
      continue
    }

    if (!fields.minute.includes(d.getMinutes())) {
      d.setMinutes(d.getMinutes() + 1, 0, 0)
      continue
    }

    return new Date(d)
  }

  return null
}
