# GameDevBuddy AI

**Stumpy** is GameDevBuddy's game-development agent. This is our controlled product fork, retaining the complete agent engine and contribution-driven client.

GameDevBuddy owns the application interface, accounts, optional services, native permissions and releases. Local and bring-your-own providers do not require a service account. Provider sign-in remains specific to the provider a person chooses.

## Full engine

The fork retains sessions and projects, persistent specialist bots, skills, memory, model/provider selection, tool execution, delegation, scheduled jobs, messaging integrations, files, voice and the existing extension system. Starter Stumpy profiles cover Unreal Engine, Unity, Godot and Narrative. Their presence is not a claim that every engine workflow has passed native acceptance.

The full renderer mounts directly into GameDevBuddy's AI activity. It uses its own bundled React runtime and scoped styles inside the application's document. It does not load a separate page or iframe.

The GameDevBuddy host authenticates HTTP requests and translates one-use socket grants and time-limited file capabilities. Private runtime credentials remain in the host's secret store. Native engine execution must continue through GameDevBuddy's authoritative native broker and approval boundaries.

## Development

Use the repository's canonical PM environment, as described in AGENTS.md. Set an explicit, dedicated HERMES_HOME and HERMES_RUNTIME_DIR before activation; never adopt a personal agent home for development.

Inside that activated environment:

```text
python -B -m hermes_cli.gamedevbuddy prepare --home <absolute-dedicated-home>
python -B -m hermes_cli.gamedevbuddy start --home <absolute-dedicated-home>
node apps/desktop/scripts/build-gamedevbuddy.mjs
python -B -m hermes_cli.gamedevbuddy stop --home <absolute-dedicated-home>
```

Preparation preserves the full default toolset and skills. It creates only our marked home and starter profiles, preserves subsequent user edits, disables upstream update checks and usage uploads, and does not adopt external account logins. Starting uses the existing canonical multiplexing gateway. Stopping uses cooperative private control and has no force-kill fallback.

The client output is `apps/desktop/dist-gamedevbuddy`. Its manifest contains the source revision, dirty-source flag and SHA-256 inventory. Generated bundles and private runtime data are not source commits. Our app consumes an explicitly pinned fork revision.

For the full companion runtime, prepare the exact commit through the canonical PM native-payload builder and build both TUI and web products. Consume those prepared inputs with `python -B -m scripts.bundles.gamedevbuddy --prepared <payload.prepared.json> --tui <built-tui> --web <built-web>`. This retains all target-compatible dependencies, tools, resources, existing commands and both frontends. It adds an isolated `bin/stumpy.py` entry, the legal notice and `gamedevbuddy-runtime.json`, which inventories every file, directory and portable internal link. It does not download dependencies or start the agent. Use a fresh assembly; published or interrupted output is never silently replaced.

GameDevBuddy's app build tooling verifies this package against its source pin and target before copying it to a fresh staging directory, then verifies the copied bytes again. The staged immutable payload is separate from the user's writable GameDevBuddy home. Signed delivery, host provisioning and real runtime acceptance remain separate work.

## Current validation boundary

The direct client, existing authenticated app API, real canonical gateway handshake and starter-profile metadata have been exercised in an isolated development home. Focused tests cover transport credential separation, one-use grants, revocation, normal host shutdown, safe configuration and preservation of user edits.

Model inference, production account/billing services, installer delivery, native engine authoring and complete desktop-host capability parity require their own acceptance. Compiling or displaying these controls does not establish those results. No upstream service is silently substituted for a missing GameDevBuddy service.

## Product identity and provenance

Application names, icons and publisher identity belong to GameDevBuddy. Windows Store packaging requires an explicit GameDevBuddy publisher and identity; upstream signing credentials are not reused.

We select upstream work deliberately. We do not automatically track or deploy upstream changes.

This fork derives from NousResearch/hermes-agent under the MIT licence. Required third-party copyright and licence notices remain in [LICENSE](LICENSE) and the retained source. The original technical introduction is preserved in [UPSTREAM_README.md](UPSTREAM_README.md).
