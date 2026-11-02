import { LOCAL_CONNECTION_ID } from '@hermes/shared'
import { useStore } from '@nanostores/react'
import { useEffect } from 'react'

import { $sessions, getSessionOwnerHint, sessionMatchesStoredId } from '@/store/session'
import { $focusedStoredSessionId } from '@/store/session-focus'

import { gameDevBuddyHost } from './host'

/** Report the actual focused durable chat to host-owned controls. This carries
 * no provider credentials, native tool, approval callback or execution permit. */
export function GameDevBuddySessionReporter() {
  const id = useStore($focusedStoredSessionId)
  const rows = useStore($sessions)
  const matches = id ? rows.filter(row => sessionMatchesStoredId(row, id)) : []
  const row = matches.length === 1 ? matches[0] : undefined
  const hint = row ? getSessionOwnerHint(row.id) : undefined
  const sessionId = row?.id
  const profile = hint?.targetProfile ?? hint?.profile ?? row?.profile ?? 'default'
  const connectionId = hint?.connectionId ?? row?.connection_id ?? LOCAL_CONNECTION_ID

  useEffect(() => {
    const host = gameDevBuddyHost()

    if (!host?.onSessionChange) {
      return
    }

    // Passive metadata stays current while the host surface is hidden. The host
    // separately checks its active surface before any selection or approval.
    host.onSessionChange(sessionId ? { sessionId, profile, connectionId } : null)

    return () => {
      if (gameDevBuddyHost() === host) {
        host.onSessionChange?.(null)
      }
    }
  }, [connectionId, profile, sessionId])

  return null
}
