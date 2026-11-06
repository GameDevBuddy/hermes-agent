"""Immutable model schema data. Possession never admits or dispatches native work."""
from dataclasses import dataclass
import json

from hermes_state_runtime import RuntimeStoreError


@dataclass(frozen=True)
class NativeModelTools:
    definitions_json: bytes

    def definitions(self):
        # A fresh request/agent-local tree; provider/cache decoration cannot write
        # through into the bound original descriptors or another agent's schemas.
        return json.loads(self.definitions_json)

    def append_to(self, configured):
        definitions = self.definitions()
        names = {tool["function"]["name"] for tool in configured or []}
        if any(tool["function"]["name"] in names for tool in definitions):
            raise RuntimeStoreError("native_model_tool_collision")
        return [*(configured or []), *definitions]
