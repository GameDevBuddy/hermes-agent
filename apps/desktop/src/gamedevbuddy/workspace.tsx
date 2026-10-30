import '../lib/browser-desktop-bridge-install'
import '../store/active-work'
import '../store/power'
import '../store/translucency'
import '../store/user-bubble-transparency'
import '../store/chat-text-scale'
import '../store/keep-awake'
import '../styles.css'

import { QueryClientProvider } from '@tanstack/react-query'
import { createRoot } from 'react-dom/client'
import { MemoryRouter } from 'react-router'
import App from '../app'
import { RootErrorBoundary } from '../components/error-boundary'
import { HapticsProvider } from '../components/haptics-provider'
import { RootTooltipProvider } from '../components/ui/tooltip'
import { ProfileI18nProvider } from '../i18n/profile-provider'
import { queryClient } from '../lib/query-client'
import { installClipboardShim } from '../lib/clipboard'
import { ThemeProvider } from '../themes/context'
import { type GameDevBuddyAiHost, gameDevBuddyHost } from './host'

/** Mount the complete contribution-driven client directly in GameDevBuddy's
 * activity area. Its React runtime is bundled independently, so other app
 * modules retain their own supported React version. No page or iframe loads. */
export function mount(host: GameDevBuddyAiHost): { dispose: () => void } {
  if (gameDevBuddyHost() !== host) {throw new Error('GameDevBuddy AI host must be bound before importing the client')}
  // The host owns the document's Clipboard API; our copy primitives already
  // use the supported browser path without replacing it for other modules.
  if (!gameDevBuddyHost()) {installClipboardShim()}
  const root = createRoot(host.element)
  root.render(
    <RootErrorBoundary>
      <QueryClientProvider client={queryClient}>
        <ProfileI18nProvider>
          <ThemeProvider>
            <HapticsProvider>
              <RootTooltipProvider>
                <MemoryRouter initialEntries={[host.route || '/']}><App /></MemoryRouter>
              </RootTooltipProvider>
            </HapticsProvider>
          </ThemeProvider>
        </ProfileI18nProvider>
      </QueryClientProvider>
    </RootErrorBoundary>
  )
  return { dispose: () => root.unmount() }
}

// A self-hosted module script avoids the host bundler rewriting this already
// compiled application's imports. This exports the same typed mount contract;
// it does not create a frame, navigate a page or expose backend credentials.
declare global {
  interface Window { __GDB_AI_CLIENT__?: { mount: typeof mount } }
}
window.__GDB_AI_CLIENT__ = { mount }
