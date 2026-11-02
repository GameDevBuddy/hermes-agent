"""Retain the original canonical claim and source objects; never release native holds."""
from __future__ import annotations

import asyncio
import threading
import weakref

from hermes_state_runtime import RuntimeStoreError


class NativeTurnRetention:
    def __init__(self, authority, ref, live, generation, task, loop):
        if not isinstance(task, asyncio.Task) or task.get_loop() is not loop or task.done():
            raise RuntimeStoreError("native_original_task_unavailable")
        self.authority, self.ref, self.live = authority, ref, live
        self.generation, self.task, self.loop = generation, task, loop
        self.lock = threading.RLock()
        self.owner = self.agent = None
        self.bridge_construction = self.bridge_construction_error = None
        self.slots, self.pins, self.active_uses = {}, {}, {}
        self.use_credit = 0
        self.ever_native = self.blocked = False
        self.completed = False

    def note_owner(self, owner):
        with self.lock:
            if self.owner is not None and self.owner is not owner:
                raise RuntimeStoreError("native_original_owner_changed")
            if (owner.scope["session_id"] != self.ref.session_id
                    or owner.scope["generation"] != self.generation):
                raise RuntimeStoreError("native_original_owner_changed")
            self.owner = owner

    def bind_owner(self, owner, agent):
        with self.lock:
            if self.owner is not owner or owner.agent() is not agent:
                raise RuntimeStoreError("native_original_owner_changed")
            if self.agent is not None and self.agent is not agent:
                raise RuntimeStoreError("native_original_agent_changed")
            self.agent = agent

    def retain_pin_attempt(self, slot):
        with self.lock:
            if (self.completed or self.blocked or slot.pending.owner is not self.owner
                    or self.owner.agent() is not self.agent):
                raise RuntimeStoreError("native_original_keeper_changed")
            prior = self.slots.get(id(slot))
            if prior is not None:
                if prior is not slot:
                    raise RuntimeStoreError("native_original_slot_changed")
                return
            if len(self.slots) >= 128:
                raise RuntimeStoreError("native_original_keeper_capacity")
            self.ever_native = True
            self.slots[id(slot)] = slot
            self.pins[id(slot)] = None

    def retain_before_send(self, slot, pin):
        with self.lock:
            if self.slots.get(id(slot)) is not slot or self.pins.get(id(slot)) is not None:
                raise RuntimeStoreError("native_original_pin_changed")
            self.pins[id(slot)] = pin

    async def wait_claim_exit(self):
        with self.lock:
            held = self.ever_native
        if held:
            from gateway.native_app_worker import require_completed_slot
            try:
                if self.blocked:
                    raise RuntimeStoreError("native_original_keeper_unknown")
                for slot in tuple(self.slots.values()):
                    require_completed_slot(slot)
            except RuntimeStoreError:
                # Unknown originals stay retained; neither timeout nor cancellation is a release.
                await asyncio.Future()
            if asyncio.current_task() is not self.task or asyncio.get_running_loop() is not self.loop:
                raise RuntimeStoreError("native_original_task_changed")
        self.completed = True


def native_retention_for_owner(owner):
    reference = getattr(owner, "_native_retention_ref", None)
    value = reference() if isinstance(reference, weakref.ReferenceType) else None
    if type(value) is not NativeTurnRetention or value.owner is not owner:
        return None
    return value


def capture_claim_retention(authority, ref, live, row):
    key = (ref.session_id, row["generation"])
    task, loop = asyncio.current_task(), asyncio.get_running_loop()
    old = authority._native_turn_retention.get(key)
    if old is not None:
        if old.live is not live or old.task is not task or old.loop is not loop:
            raise RuntimeStoreError("native_original_task_changed")
        return old
    if len(authority._native_turn_retention) >= 1024:
        raise RuntimeStoreError("native_original_turn_capacity")
    value = NativeTurnRetention(authority, ref, live, row["generation"], task, loop)
    authority._native_turn_retention[key] = value
    return value


def install_owner_retention(authority, session_id, generation, owner):
    value = getattr(authority, "_native_turn_retention", {}).get((session_id, generation))
    owner._native_retention_ref = None
    if value is not None:
        try:
            value.note_owner(owner)
        except RuntimeStoreError:
            return  # Ordinary commit observation does not acquire another native owner.
        owner._native_retention_ref = weakref.ref(value)
