"""Managed foreground route; source integration is incomplete and stays unavailable.

The normal lifecycle must be composed at the existing canonical consumers before
this entry may construct/start a gateway. Original streams/Job custody are held
by the caller on this refusal; no legacy shutdown or force-exit path is entered.
"""
from __future__ import annotations


def main(source):
    from hermes_owned_gateway import OwnedGatewayUnknown
    source.require_attached(source.contract)
    raise OwnedGatewayUnknown("canonical-managed-normal-lifecycle-not-composed")
