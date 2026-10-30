"""Canonical selection staging only; the original stdin must complete association."""
from typing import Literal

from .base import Params, Result
from .registry import canonical_method


class NativeContextTool(Params):
    name: str
    tool_ref: str


class NativeContextStageParams(Params):
    nonce: str
    session_id: str
    context_id: str
    snapshot_sha256: str
    tools: list[NativeContextTool]


class NativeContextStageResult(Result):
    staged: Literal[True]
    context_id: str
    nonce: str


canonical_method("native.context.stage", params=NativeContextStageParams, result=NativeContextStageResult,
                 doc="Stage a verified canonical context selection for the original owned stdin. "
                     "No native work, per-call authority or execution is admitted.")
