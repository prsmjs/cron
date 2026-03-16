import { describe, it, expect } from 'vitest'
import { parseCronExpression, nextCronTime } from '../src/parse.js'

describe('parseCronExpression', () => {
  describe('basic fields', () => {
    it('parses wildcard in all fields', () => {
      const f = parseCronExpression('* * * * *')
      expect(f.minute).toEqual(Array.from({ length: 60 }, (_, i) => i))
      expect(f.hour).toEqual(Array.from({ length: 24 }, (_, i) => i))
      expect(f.dom).toEqual(Array.from({ length: 31 }, (_, i) => i + 1))
      expect(f.month).toEqual(Array.from({ length: 12 }, (_, i) => i + 1))
      expect(f.dow).toEqual([0, 1, 2, 3, 4, 5, 6])
    })

    it('parses specific values', () => {
      const f = parseCronExpression('30 14 1 6 3')
      expect(f.minute).toEqual([30])
      expect(f.hour).toEqual([14])
      expect(f.dom).toEqual([1])
      expect(f.month).toEqual([6])
      expect(f.dow).toEqual([3])
    })
  })

  describe('ranges', () => {
    it('parses a simple range', () => {
      const f = parseCronExpression('1-5 * * * *')
      expect(f.minute).toEqual([1, 2, 3, 4, 5])
    })

    it('parses a range in hour field', () => {
      const f = parseCronExpression('0 9-17 * * *')
      expect(f.hour).toEqual([9, 10, 11, 12, 13, 14, 15, 16, 17])
    })
  })

  describe('steps', () => {
    it('parses */N', () => {
      const f = parseCronExpression('*/15 * * * *')
      expect(f.minute).toEqual([0, 15, 30, 45])
    })

    it('parses range with step', () => {
      const f = parseCronExpression('1-10/3 * * * *')
      expect(f.minute).toEqual([1, 4, 7, 10])
    })

    it('parses N/S as start-at-N step-by-S through max', () => {
      const f = parseCronExpression('5/10 * * * *')
      expect(f.minute).toEqual([5, 15, 25, 35, 45, 55])
    })
  })

  describe('lists', () => {
    it('parses comma-separated values', () => {
      const f = parseCronExpression('0,15,30,45 * * * *')
      expect(f.minute).toEqual([0, 15, 30, 45])
    })

    it('parses mixed list with ranges and steps', () => {
      const f = parseCronExpression('1-3,10,20-25/2 * * * *')
      expect(f.minute).toEqual([1, 2, 3, 10, 20, 22, 24])
    })

    it('deduplicates overlapping values', () => {
      const f = parseCronExpression('1-5,3-7 * * * *')
      expect(f.minute).toEqual([1, 2, 3, 4, 5, 6, 7])
    })
  })

  describe('month names', () => {
    it('parses three-letter month abbreviations', () => {
      const f = parseCronExpression('0 0 1 jan *')
      expect(f.month).toEqual([1])
    })

    it('parses month name ranges', () => {
      const f = parseCronExpression('0 0 1 mar-jun *')
      expect(f.month).toEqual([3, 4, 5, 6])
    })

    it('is case insensitive', () => {
      const f = parseCronExpression('0 0 1 JAN,FEB,MAR *')
      expect(f.month).toEqual([1, 2, 3])
    })
  })

  describe('day-of-week names', () => {
    it('parses three-letter day abbreviations', () => {
      const f = parseCronExpression('0 0 * * mon')
      expect(f.dow).toEqual([1])
    })

    it('parses day name ranges', () => {
      const f = parseCronExpression('0 0 * * mon-fri')
      expect(f.dow).toEqual([1, 2, 3, 4, 5])
    })

    it('normalizes day 7 to 0 (both mean Sunday)', () => {
      const f = parseCronExpression('0 0 * * 7')
      expect(f.dow).toEqual([0])
    })

    it('deduplicates 0 and 7', () => {
      const f = parseCronExpression('0 0 * * 0,7')
      expect(f.dow).toEqual([0])
    })
  })

  describe('wildcards tracking', () => {
    it('marks dom as wild when *', () => {
      const f = parseCronExpression('0 0 * * 1')
      expect(f.domWild).toBe(true)
      expect(f.dowWild).toBe(false)
    })

    it('marks dow as wild when *', () => {
      const f = parseCronExpression('0 0 15 * *')
      expect(f.domWild).toBe(false)
      expect(f.dowWild).toBe(true)
    })

    it('marks both as wild', () => {
      const f = parseCronExpression('0 0 * * *')
      expect(f.domWild).toBe(true)
      expect(f.dowWild).toBe(true)
    })
  })

  describe('shortcuts', () => {
    it('@yearly', () => {
      const f = parseCronExpression('@yearly')
      expect(f.minute).toEqual([0])
      expect(f.hour).toEqual([0])
      expect(f.dom).toEqual([1])
      expect(f.month).toEqual([1])
    })

    it('@monthly', () => {
      const f = parseCronExpression('@monthly')
      expect(f.minute).toEqual([0])
      expect(f.hour).toEqual([0])
      expect(f.dom).toEqual([1])
      expect(f.month).toEqual(Array.from({ length: 12 }, (_, i) => i + 1))
    })

    it('@weekly', () => {
      const f = parseCronExpression('@weekly')
      expect(f.dow).toEqual([0])
    })

    it('@daily', () => {
      const f = parseCronExpression('@daily')
      expect(f.minute).toEqual([0])
      expect(f.hour).toEqual([0])
    })

    it('@hourly', () => {
      const f = parseCronExpression('@hourly')
      expect(f.minute).toEqual([0])
      expect(f.hour).toEqual(Array.from({ length: 24 }, (_, i) => i))
    })

    it('@annually is same as @yearly', () => {
      const a = parseCronExpression('@annually')
      const b = parseCronExpression('@yearly')
      expect(a).toEqual(b)
    })

    it('@midnight is same as @daily', () => {
      const a = parseCronExpression('@midnight')
      const b = parseCronExpression('@daily')
      expect(a).toEqual(b)
    })
  })

  describe('validation', () => {
    it('rejects wrong number of fields', () => {
      expect(() => parseCronExpression('* * *')).toThrow('exactly 5 fields')
      expect(() => parseCronExpression('* * * * * *')).toThrow('exactly 5 fields')
    })

    it('rejects out-of-range values', () => {
      expect(() => parseCronExpression('60 * * * *')).toThrow('out of range')
      expect(() => parseCronExpression('* 24 * * *')).toThrow('out of range')
      expect(() => parseCronExpression('* * 0 * *')).toThrow('out of range')
      expect(() => parseCronExpression('* * 32 * *')).toThrow('out of range')
      expect(() => parseCronExpression('* * * 0 *')).toThrow('out of range')
      expect(() => parseCronExpression('* * * 13 *')).toThrow('out of range')
      expect(() => parseCronExpression('* * * * 8')).toThrow('out of range')
    })

    it('rejects invalid step', () => {
      expect(() => parseCronExpression('*/0 * * * *')).toThrow('invalid step')
      expect(() => parseCronExpression('*/abc * * * *')).toThrow('invalid step')
    })

    it('rejects inverted ranges', () => {
      expect(() => parseCronExpression('5-2 * * * *')).toThrow('invalid cron range')
    })

    it('rejects non-numeric values', () => {
      expect(() => parseCronExpression('abc * * * *')).toThrow('invalid cron value')
    })
  })
})

describe('nextCronTime', () => {
  it('finds the next minute for * * * * *', () => {
    const now = new Date(2026, 2, 15, 10, 30, 0)
    const fields = parseCronExpression('* * * * *')
    const next = nextCronTime(fields, now)
    expect(next.getMinutes()).toBe(31)
    expect(next.getHours()).toBe(10)
  })

  it('finds the next matching minute', () => {
    const now = new Date(2026, 2, 15, 10, 30, 0)
    const fields = parseCronExpression('45 * * * *')
    const next = nextCronTime(fields, now)
    expect(next.getMinutes()).toBe(45)
    expect(next.getHours()).toBe(10)
  })

  it('wraps to the next hour', () => {
    const now = new Date(2026, 2, 15, 10, 50, 0)
    const fields = parseCronExpression('15 * * * *')
    const next = nextCronTime(fields, now)
    expect(next.getMinutes()).toBe(15)
    expect(next.getHours()).toBe(11)
  })

  it('wraps to the next day', () => {
    const now = new Date(2026, 2, 15, 23, 50, 0)
    const fields = parseCronExpression('0 9 * * *')
    const next = nextCronTime(fields, now)
    expect(next.getMinutes()).toBe(0)
    expect(next.getHours()).toBe(9)
    expect(next.getDate()).toBe(16)
  })

  it('wraps to the next month', () => {
    const now = new Date(2026, 2, 31, 23, 59, 0)
    const fields = parseCronExpression('0 0 1 * *')
    const next = nextCronTime(fields, now)
    expect(next.getDate()).toBe(1)
    expect(next.getMonth()).toBe(3)
  })

  it('wraps to the next year', () => {
    const now = new Date(2026, 11, 31, 23, 59, 0)
    const fields = parseCronExpression('0 0 1 1 *')
    const next = nextCronTime(fields, now)
    expect(next.getFullYear()).toBe(2027)
    expect(next.getMonth()).toBe(0)
    expect(next.getDate()).toBe(1)
  })

  it('finds the next matching day-of-week', () => {
    const now = new Date(2026, 2, 15, 0, 0, 0) // Sunday
    const fields = parseCronExpression('0 9 * * mon')
    const next = nextCronTime(fields, now)
    expect(next.getDay()).toBe(1)
    expect(next.getDate()).toBe(16)
  })

  it('handles day-of-month and day-of-week OR logic', () => {
    // both dom (1) and dow (0=Sunday) specified - OR behavior
    const now = new Date(2026, 2, 14, 0, 0, 0) // Saturday
    const fields = parseCronExpression('0 0 1 * 0')
    const next = nextCronTime(fields, now)
    // next Sunday is March 15
    expect(next.getDate()).toBe(15)
    expect(next.getDay()).toBe(0)
  })

  it('finds Feb 29 on leap year', () => {
    const now = new Date(2027, 0, 1, 0, 0, 0)
    const fields = parseCronExpression('0 0 29 2 *')
    const next = nextCronTime(fields, now)
    expect(next).not.toBeNull()
    expect(next.getFullYear()).toBe(2028)
    expect(next.getMonth()).toBe(1)
    expect(next.getDate()).toBe(29)
  })

  it('handles */5 minute schedule', () => {
    const now = new Date(2026, 2, 15, 10, 7, 0)
    const fields = parseCronExpression('*/5 * * * *')
    const next = nextCronTime(fields, now)
    expect(next.getMinutes()).toBe(10)
  })

  it('returns null when no match within 4 years', () => {
    // dom=31, month=2 - February never has 31 days
    const fields = parseCronExpression('0 0 31 2 *')
    const next = nextCronTime(fields, new Date(2026, 0, 1))
    expect(next).toBeNull()
  })

  it('accepts a numeric timestamp', () => {
    const ts = new Date(2026, 2, 15, 10, 30, 0).getTime()
    const fields = parseCronExpression('45 * * * *')
    const next = nextCronTime(fields, ts)
    expect(next.getMinutes()).toBe(45)
  })

  it('never returns the current minute', () => {
    const now = new Date(2026, 2, 15, 10, 30, 0)
    const fields = parseCronExpression('30 10 * * *')
    const next = nextCronTime(fields, now)
    expect(next.getDate()).toBe(16)
  })

  it('handles @hourly shortcut', () => {
    const now = new Date(2026, 2, 15, 10, 30, 0)
    const fields = parseCronExpression('@hourly')
    const next = nextCronTime(fields, now)
    expect(next.getMinutes()).toBe(0)
    expect(next.getHours()).toBe(11)
  })

  it('handles complex expression: weekdays at 9:30', () => {
    const now = new Date(2026, 2, 13, 10, 0, 0) // Friday
    const fields = parseCronExpression('30 9 * * 1-5')
    const next = nextCronTime(fields, now)
    // Friday 10:00 already past 9:30, so next is Monday March 16
    expect(next.getDay()).toBe(1)
    expect(next.getHours()).toBe(9)
    expect(next.getMinutes()).toBe(30)
  })
})
