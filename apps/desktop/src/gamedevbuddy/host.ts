/** The first-party GameDevBuddy host. No runtime or provider credentials cross
 * this contract. All HTTP requests retain the app's current workspace binding. */
export interface GameDevBuddyAiHost {
  element: HTMLElement
  apiBase: string
  profile?: string
  route?: string
  windowUrl?: (target: string) => string
  request: (path: string, init: RequestInit) => Promise<Response>
  socketUrl: (ticket: string) => string
  fileUrl: (ticket: string) => string
  openAccount: () => void
  isActive: () => boolean
}

declare global {
  interface Window { __GDB_AI_HOST__?: GameDevBuddyAiHost }
}

export function gameDevBuddyHost(): GameDevBuddyAiHost | undefined {
  return typeof window === 'undefined' ? undefined : window.__GDB_AI_HOST__
}

/** Renderer appearance/portals belong to this workspace, not the host app. */
export function desktopSurface(): HTMLElement {
  return gameDevBuddyHost()?.element ?? document.documentElement
}

export function desktopSurfaceBody(): HTMLElement {
  return gameDevBuddyHost()?.element ?? document.body
}

/** Document-level input must stay with its owning GameDevBuddy surface. */
export function ownsDesktopEvent(event: Event): boolean {
  const host = gameDevBuddyHost()
  return !host || (host.isActive() && event.target instanceof Node && host.element.contains(event.target))
}
