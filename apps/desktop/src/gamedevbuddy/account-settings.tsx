import { Button } from '@/components/ui/button'
import { gameDevBuddyHost } from './host'

/** Product identity and hosted entitlements belong to GameDevBuddy. Provider
 * accounts remain configurable independently in the complete Models settings. */
export function GameDevBuddyAccountSettings() {
  return <section className="space-y-5 p-6">
    <h2 className="text-lg font-semibold">GameDevBuddy account</h2>
    <p>Local AI and your own model connections work without a service account.</p>
    <p>Use your GameDevBuddy account for optional hosted services.</p>
    <Button onClick={() => gameDevBuddyHost()?.openAccount()}>Manage GameDevBuddy account</Button>
  </section>
}
