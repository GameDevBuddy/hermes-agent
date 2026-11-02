import { act, cleanup, render, waitFor } from '@testing-library/react'
import type { WritableAtom } from 'nanostores'
import { afterEach, expect, it, vi } from 'vitest'

vi.mock('@/store/session', async () => {
  const { atom } = await import('nanostores')

  return {
    $sessions: atom([]),
    getSessionOwnerHint: () => undefined,
    sessionMatchesStoredId: (row: { id: string }, id: string) => row.id === id
  }
})
vi.mock('@/store/session-focus', async () => {
  const { atom } = await import('nanostores')

  return { $focusedStoredSessionId: atom(null) }
})

import { $sessions } from '@/store/session'
import { $focusedStoredSessionId } from '@/store/session-focus'
import type { SessionInfo } from '@/types/hermes'

import type { GameDevBuddyAiHost } from './host'
import { GameDevBuddySessionReporter } from './session-reporter'

const focus = $focusedStoredSessionId as WritableAtom<null | string>

afterEach(() => {
  cleanup()
  delete window.__GDB_AI_HOST__
  $sessions.set([])
  focus.set(null)
})

it('reports only the exact focused stored chat and clears it when its owner unmounts', async () => {
  const changed = vi.fn()
  window.__GDB_AI_HOST__ = { onSessionChange: changed, isActive: () => true } as unknown as GameDevBuddyAiHost
  const view = render(<GameDevBuddySessionReporter />)
  act(() => {
    $sessions.set([{ id: 'actual-chat', profile: 'default', connection_id: 'local' }] as SessionInfo[])
    focus.set('actual-chat')
  })
  await waitFor(() =>
    expect(changed).toHaveBeenLastCalledWith({ sessionId: 'actual-chat', profile: 'default', connectionId: 'local' })
  )
  act(() => {
    focus.set('unloaded-chat')
  })
  await waitFor(() => expect(changed).toHaveBeenLastCalledWith(null))
  view.unmount()
  expect(changed).toHaveBeenLastCalledWith(null)
})

it('withholds ambiguous owners and never clears a replacement host', () => {
  const changed = vi.fn(),
    replacement = vi.fn()

  window.__GDB_AI_HOST__ = { onSessionChange: changed, isActive: () => true } as unknown as GameDevBuddyAiHost
  $sessions.set([
    { id: 'same', profile: 'one' },
    { id: 'same', profile: 'two' }
  ] as SessionInfo[])
  focus.set('same')
  const view = render(<GameDevBuddySessionReporter />)
  expect(changed).toHaveBeenLastCalledWith(null)
  window.__GDB_AI_HOST__ = { onSessionChange: replacement, isActive: () => true } as unknown as GameDevBuddyAiHost
  view.unmount()
  expect(replacement).not.toHaveBeenCalled()
})

it('keeps passive chat metadata current while the host surface is hidden', () => {
  const changed = vi.fn()

  window.__GDB_AI_HOST__ = { onSessionChange: changed, isActive: () => false } as unknown as GameDevBuddyAiHost
  $sessions.set([{ id: 'hidden-chat', profile: 'default', connection_id: 'local' }] as SessionInfo[])
  focus.set('hidden-chat')
  render(<GameDevBuddySessionReporter />)
  expect(changed).toHaveBeenLastCalledWith({ sessionId: 'hidden-chat', profile: 'default', connectionId: 'local' })
})
