import { createWechatDaemonAutoReplyHandler } from './wechatDaemonAutoReplyHandler.js'
import { createWechatDaemonRuntime } from './wechatDaemonRuntime.js'

const OWNER_NAME = 'global'
const CONTINUE_DELAY_MS = 800
const ERROR_DELAY_MS = 2000
const REMOTE_DEBUG_EVENT_URL = 'https://ai-phone-background.yutuyue2.workers.dev/debug/event'

const normalizeText = (value = '') => String(value || '').trim()

const reportDebugEvent = (location = '', msg = '', data = {}) => {
  fetch(REMOTE_DEBUG_EVENT_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      sessionId: 'wechat-auto-reply',
      runId: 'pre-fix',
      hypothesisId: 'H1',
      location,
      msg,
      data,
      ts: Date.now()
    })
  }).catch(() => {})
}

const json = (payload = {}, status = 200) => new Response(JSON.stringify(payload), {
  status,
  headers: {
    'content-type': 'application/json; charset=utf-8'
  }
})

const isWechatDaemonEnabled = (env = {}) => ['1', 'true'].includes(
  normalizeText(env?.WECHAT_DAEMON_ENABLED).toLowerCase()
)

const isActiveBinding = (binding = {}) => {
  const bindingId = normalizeText(binding?.bindingId || binding?.remoteBindingId)
  const bindingStatus = normalizeText(binding?.status)
  if (!bindingId) return false
  return !bindingStatus || ['bound', 'pending'].includes(bindingStatus)
}

const createWechatDaemonRuntimeForOwner = (env = {}) => createWechatDaemonRuntime({
  ...env,
  __WECHAT_DAEMON_AUTO_REPLY_HANDLER__: createWechatDaemonAutoReplyHandler(env)
})

export class WechatDaemonLongPollOwner {
  constructor(state, env) {
    this.state = state
    this.env = env
    this.loopPromise = null
  }

  async fetch(request) {
    try {
      const url = new URL(request.url)
      if (url.pathname === '/status') {
        return json(await this.getStatus())
      }
      if (url.pathname === '/ensure') {
        return json(await this.ensure())
      }
      return json({ ok: false, error: 'route_not_found' }, 404)
    } catch (error) {
      const safeError = normalizeText(error?.message || error)
      // #region debug-point H1:owner-fetch-failed
      reportDebugEvent('workers/personal-runtime/src/wechat/wechatDaemonLongPollOwner.js:fetch', '[DEBUG] long-poll owner fetch failed', {
        error: safeError
      })
      // #endregion
      return json({
        ok: false,
        owner: 'wechat-daemon-long-poll',
        error: safeError || 'wechat_daemon_long_poll_fetch_failed'
      }, 500)
    }
  }

  async alarm() {
    try {
      await this.runLoop('alarm')
    } catch (error) {
      const safeError = normalizeText(error?.message || error)
      // #region debug-point H1:owner-alarm-failed
      reportDebugEvent('workers/personal-runtime/src/wechat/wechatDaemonLongPollOwner.js:alarm', '[DEBUG] long-poll owner alarm failed', {
        error: safeError
      })
      // #endregion
      await this.state.storage.put({
        lastRunAt: Date.now(),
        lastTrigger: 'alarm',
        lastError: safeError
      }).catch(() => null)
    }
  }

  async ensure() {
    if (!isWechatDaemonEnabled(this.env)) {
      return { ok: false, skipped: 'daemon_disabled' }
    }
    const runtime = createWechatDaemonRuntimeForOwner(this.env)
    const bindings = await runtime.store.listBindings().catch(() => [])
    const activeBindingCount = (Array.isArray(bindings) ? bindings : []).filter(isActiveBinding).length
    // #region debug-point H1:owner-ensure
    reportDebugEvent('workers/personal-runtime/src/wechat/wechatDaemonLongPollOwner.js:ensure', '[DEBUG] long-poll owner ensure', {
      activeBindingCount
    })
    // #endregion
    await this.state.storage.put({
      lastEnsureAt: Date.now(),
      activeBindingCount
    })
    if (activeBindingCount > 0) {
      await this.state.storage.setAlarm(Date.now())
    }
    return {
      ok: true,
      activeBindingCount
    }
  }

  async runLoop(trigger = 'alarm') {
    if (this.loopPromise) {
      return this.loopPromise
    }
    this.loopPromise = (async () => {
      let activeBindingCount = 0
      let lastError = ''
      let nextDelayMs = 0
      try {
        if (!isWechatDaemonEnabled(this.env)) {
          return
        }
        const runtime = createWechatDaemonRuntimeForOwner(this.env)
        const beforeBindings = await runtime.store.listBindings().catch(() => [])
        activeBindingCount = (Array.isArray(beforeBindings) ? beforeBindings : []).filter(isActiveBinding).length
        // #region debug-point H1:owner-runloop-start
        reportDebugEvent('workers/personal-runtime/src/wechat/wechatDaemonLongPollOwner.js:runLoop:start', '[DEBUG] long-poll owner runLoop start', {
          trigger,
          activeBindingCount
        })
        // #endregion
        if (activeBindingCount <= 0) {
          await this.state.storage.put({
            lastRunAt: Date.now(),
            lastTrigger: trigger,
            activeBindingCount: 0,
            lastError: ''
          })
          return
        }
        await runtime.tick({
          syncBindings: true,
          inlineQuietWaitMs: 0
        })
        const afterBindings = await runtime.store.listBindings().catch(() => beforeBindings)
        activeBindingCount = (Array.isArray(afterBindings) ? afterBindings : []).filter(isActiveBinding).length
        nextDelayMs = activeBindingCount > 0 ? CONTINUE_DELAY_MS : 0
        // #region debug-point H1:owner-runloop-success
        reportDebugEvent('workers/personal-runtime/src/wechat/wechatDaemonLongPollOwner.js:runLoop:success', '[DEBUG] long-poll owner runLoop success', {
          trigger,
          activeBindingCount,
          nextDelayMs
        })
        // #endregion
        await this.state.storage.put({
          lastRunAt: Date.now(),
          lastTrigger: trigger,
          activeBindingCount,
          lastError: ''
        })
      } catch (error) {
        lastError = normalizeText(error?.message || error)
        nextDelayMs = ERROR_DELAY_MS
        // #region debug-point H1:owner-runloop-failed
        reportDebugEvent('workers/personal-runtime/src/wechat/wechatDaemonLongPollOwner.js:runLoop:failed', '[DEBUG] long-poll owner runLoop failed', {
          trigger,
          activeBindingCount,
          nextDelayMs,
          error: lastError
        })
        // #endregion
        await this.state.storage.put({
          lastRunAt: Date.now(),
          lastTrigger: trigger,
          activeBindingCount,
          lastError
        })
      } finally {
        this.loopPromise = null
        if (activeBindingCount > 0 && isWechatDaemonEnabled(this.env)) {
          await this.state.storage.setAlarm(Date.now() + nextDelayMs)
        }
      }
    })()
    return this.loopPromise
  }

  async getStatus() {
    const [lastRunAt, lastEnsureAt, lastTrigger, activeBindingCount, lastError, alarmAt] = await Promise.all([
      this.state.storage.get('lastRunAt'),
      this.state.storage.get('lastEnsureAt'),
      this.state.storage.get('lastTrigger'),
      this.state.storage.get('activeBindingCount'),
      this.state.storage.get('lastError'),
      this.state.storage.getAlarm()
    ])
    return {
      ok: true,
      owner: 'wechat-daemon-long-poll',
      enabled: isWechatDaemonEnabled(this.env),
      activeBindingCount: Number(activeBindingCount || 0),
      running: !!this.loopPromise,
      lastRunAt: Number(lastRunAt || 0),
      lastEnsureAt: Number(lastEnsureAt || 0),
      lastTrigger: normalizeText(lastTrigger),
      lastError: normalizeText(lastError),
      nextAlarmAt: Number(alarmAt || 0)
    }
  }
}

const getOwnerStub = (env = {}) => {
  const namespace = env.WECHAT_DAEMON_LONG_POLL_OWNER
  if (!namespace) return null
  const id = namespace.idFromName(OWNER_NAME)
  return namespace.get(id)
}

const readJsonResponse = async (response) => {
  const text = await response.text().catch(() => '')
  if (!text) return {}
  try {
    return JSON.parse(text)
  } catch {
    return { ok: response.ok, text }
  }
}

export const ensureWechatDaemonLongPollOwner = async (env = {}, ctx = null) => {
  const stub = getOwnerStub(env)
  if (!stub) return { ok: false, skipped: 'missing_long_poll_owner_binding' }
  const run = async () => {
    const response = await stub.fetch('https://wechat-daemon-long-poll/ensure', { method: 'POST' })
    const payload = await readJsonResponse(response)
    // #region debug-point H1:ensure-stub-response
    reportDebugEvent('workers/personal-runtime/src/wechat/wechatDaemonLongPollOwner.js:ensureWechatDaemonLongPollOwner', '[DEBUG] long-poll owner ensure stub response', {
      status: Number(response?.status || 0),
      ok: response?.ok === true,
      payload
    })
    // #endregion
    return payload
  }
  if (ctx?.waitUntil) {
    ctx.waitUntil(run().catch((error) => {
      // #region debug-point H1:ensure-stub-failed
      reportDebugEvent('workers/personal-runtime/src/wechat/wechatDaemonLongPollOwner.js:ensureWechatDaemonLongPollOwner', '[DEBUG] long-poll owner ensure stub failed', {
        error: normalizeText(error?.message || error)
      })
      // #endregion
    }))
    return { ok: true, scheduled: true }
  }
  return run()
}

export const hasWechatDaemonLongPollOwner = (env = {}) => !!getOwnerStub(env)

export const getWechatDaemonLongPollOwnerStatus = async (env = {}) => {
  const stub = getOwnerStub(env)
  if (!stub) return { ok: false, skipped: 'missing_long_poll_owner_binding' }
  const response = await stub.fetch('https://wechat-daemon-long-poll/status')
  const payload = await readJsonResponse(response)
  // #region debug-point H1:status-stub-response
  reportDebugEvent('workers/personal-runtime/src/wechat/wechatDaemonLongPollOwner.js:getWechatDaemonLongPollOwnerStatus', '[DEBUG] long-poll owner status stub response', {
    status: Number(response?.status || 0),
    ok: response?.ok === true,
    payload
  })
  // #endregion
  return payload
}

export const isWechatDaemonLongPollOwnerHealthy = async (env = {}, {
  maxIdleMs = 15000
} = {}) => {
  const status = await getWechatDaemonLongPollOwnerStatus(env).catch((error) => ({
    ok: false,
    error: normalizeText(error?.message || error)
  }))
  const now = Date.now()
  const lastRunAt = Number(status?.lastRunAt || 0)
  const nextAlarmAt = Number(status?.nextAlarmAt || 0)
  const activeBindingCount = Number(status?.activeBindingCount || 0)
  const lastError = normalizeText(status?.lastError)
  const recentlyRan = lastRunAt > 0 && now - lastRunAt <= Math.max(1000, Number(maxIdleMs || 15000))
  const alarmPending = nextAlarmAt > 0 && nextAlarmAt <= now + Math.max(1000, Number(maxIdleMs || 15000))
  return {
    ok: true,
    healthy: status?.ok === true
      && status?.enabled === true
      && activeBindingCount > 0
      && !lastError
      && (status?.running === true || recentlyRan || alarmPending),
    status
  }
}
