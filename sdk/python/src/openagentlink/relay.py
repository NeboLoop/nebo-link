"""Proving this device's key to a relay (crates/oal-relay, "Proving a key").

Before every connection the relay wants a fresh proof that the device holds its
X25519 key: ``GET /oal/challenge`` for a single-use nonce and the relay's key,
then::

    shared = X25519(device_secret, relayKey)
    prk    = HMAC-SHA256(key = "oal-relay-auth/1", message = shared)
    proof  = HMAC-SHA256(key = prk, message =
               "oal-relay-auth/1\\n" role "\\n" deviceKey "\\n" relayKey "\\n" nonce "\\n" target "\\n")

sent as the ``key``, ``nonce`` and ``proof`` query parameters (a browser cannot
set headers on a WebSocket). The key is the same static key OAL's end-to-end
encryption uses.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import ipaddress
import json
import re
import urllib.request
from typing import Any
from urllib.parse import urlencode, urlsplit, urlunsplit

from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey, X25519PublicKey

from .channel import Dialer, Socket
from .errors import HostOffline, InvalidParams

_DOMAIN = "oal-relay-auth/1"
_ROUTE = re.compile(r"^(.*)/oal/(hosts|pair)/([^/]+)$")


def relay_dialer(relay: str, device: dict[str, str], dialer: Dialer) -> Dialer:
    """Wraps ``dialer`` so that every connection to ``relay`` carries a proof of
    ``device``'s key (``{"publicKey", "privateKey"}``, base64url). Refuses a
    relay that isn't ``wss://``, except one on this machine."""
    _check_relay(relay)

    async def dial(url: str, protocols: list[str]) -> Socket:
        parts = urlsplit(url)
        route = _ROUTE.match(parts.path)
        if route is None:
            raise InvalidParams(f"That isn't a relay connection: {url}")
        prefix, kind, name = route.groups()
        scheme = "https" if parts.scheme == "wss" else "http"
        challenge = urlunsplit((scheme, parts.netloc, prefix + "/oal/challenge", "", ""))
        nonce, relay_key = await asyncio.to_thread(_challenge, challenge, parts.netloc)
        target = ("connect:" if kind == "hosts" else "pair:") + name
        proof = _prove(device, relay_key, nonce, target)
        query = urlencode({"key": device["publicKey"], "nonce": nonce, "proof": proof})
        return await dialer(urlunsplit(parts._replace(query=query)), protocols)

    return dial


def _check_relay(relay: str) -> None:
    parts = urlsplit(relay if re.match(r"^wss?://", relay) else f"wss://{relay}")
    host = parts.hostname or ""
    try:
        local = host == "localhost" or ipaddress.ip_address(host).is_loopback
    except ValueError:
        local = False
    if parts.scheme != "wss" and not local:
        raise InvalidParams(f"{relay} isn't encrypted. Use wss:// (or ws:// only for a relay on this machine).")


def _challenge(url: str, netloc: str) -> tuple[str, str]:
    try:
        with urllib.request.urlopen(url, timeout=10) as response:
            body: Any = json.load(response)
    except (OSError, ValueError):
        raise HostOffline(f"Couldn't reach {netloc}.") from None
    nonce, relay_key = (body.get("nonce"), body.get("relayKey")) if isinstance(body, dict) else (None, None)
    if not isinstance(nonce, str) or not isinstance(relay_key, str):
        raise HostOffline(f"Couldn't reach {netloc}.")
    return nonce, relay_key


def _prove(device: dict[str, str], relay_key: str, nonce: str, target: str) -> str:
    """The proof for ``target`` (``connect:<hostId>``, ``pair:<NAMEPLATE>``), as a client."""
    secret = X25519PrivateKey.from_private_bytes(_unb64url(device["privateKey"]))
    try:
        shared = secret.exchange(X25519PublicKey.from_public_bytes(_unb64url(relay_key)))
    except ValueError:
        raise HostOffline("The relay's key isn't usable.") from None
    prk = hmac.digest(_DOMAIN.encode(), shared, hashlib.sha256)
    transcript = "".join(f"{part}\n" for part in (_DOMAIN, "client", device["publicKey"], relay_key, nonce, target))
    return b64url(hmac.digest(prk, transcript.encode(), hashlib.sha256))


def _unb64url(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


def b64url(data: bytes) -> str:
    """Base64url without padding, the way OAL writes keys."""
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()
