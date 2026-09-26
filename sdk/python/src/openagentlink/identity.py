"""Pairing (spec section 6): a one-time code becomes a device identity."""

from __future__ import annotations

import base64
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, NoEncryption, PrivateFormat, PublicFormat

from .channel import ChannelContext, Dialer, SecureChannel, plaintext, websocket_dialer
from .connection import exchange
from .errors import InvalidParams
from .types import PROTOCOL, ClientInfo, VersionRange

DEFAULT_CLIENT: ClientInfo = {"name": "openagentlink-python", "version": "0.1.0"}


@dataclass(frozen=True)
class HostIdentity:
    id: str
    name: str
    public_key: str


@dataclass(frozen=True)
class DeviceIdentity:
    id: str
    name: str
    token: str
    public_key: str
    private_key: str


@dataclass(frozen=True)
class Identity:
    """What pairing with one host produces. Keep it secret: it holds the device
    token and private key. Pass it to ``connect`` as ``credentials``."""

    host: HostIdentity
    device: DeviceIdentity

    def to_dict(self) -> dict[str, Any]:
        """The identity as JSON-ready data, in the same shape the TypeScript SDK stores."""
        h, d = self.host, self.device
        return {
            "host": {"id": h.id, "name": h.name, "publicKey": h.public_key},
            "device": {"id": d.id, "name": d.name, "token": d.token, "publicKey": d.public_key, "privateKey": d.private_key},
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Identity:
        h, d = data["host"], data["device"]
        return cls(
            host=HostIdentity(id=h["id"], name=h["name"], public_key=h["publicKey"]),
            device=DeviceIdentity(
                id=d["id"], name=d["name"], token=d["token"], public_key=d["publicKey"], private_key=d["privateKey"]
            ),
        )

    def save(self, path: str | os.PathLike[str]) -> None:
        """Writes the identity as JSON, readable only by this user."""
        target = Path(path).expanduser()
        target.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as file:
            json.dump(self.to_dict(), file, indent=2)

    @classmethod
    def load(cls, path: str | os.PathLike[str]) -> Identity:
        return cls.from_dict(json.loads(Path(path).expanduser().read_text()))


def relay_url(relay: str, path: str) -> str:
    """A relay path (``/oal/hosts/<id>``, ``/oal/pair/<nameplate>``) on ``relay``."""
    base = relay if re.match(r"^wss?://", relay) else f"wss://{relay}"
    return base.rstrip("/") + path


def nameplate(code: str) -> str:
    """The code's nameplate (spec 4.4, 6.2): its first four characters, read as
    Crockford base32. A relay routes a pairing by the nameplate alone; the rest
    of the code never goes to it."""
    normal = re.sub(r"[^0-9A-Z]", "", code.upper())
    return normal.replace("I", "1").replace("L", "1").replace("O", "0")[:4]


def check_endpoint(relay: str | None, url: str | None) -> None:
    if (relay is None) == (url is None):
        raise InvalidParams("Pass either relay or url.")


async def pair(
    *,
    code: str,
    device_name: str,
    relay: str | None = None,
    url: str | None = None,
    client: ClientInfo = DEFAULT_CLIENT,
    secure: SecureChannel = plaintext,
    dialer: Dialer = websocket_dialer,
) -> Identity:
    """Pairs with a host through ``relay`` or at ``url`` and returns this device's identity for it."""
    check_endpoint(relay, url)
    target = relay_url(relay, "/oal/pair/" + nameplate(code)) if relay else url
    assert target is not None
    public_key, private_key = _generate_key_pair()
    socket = await dialer(target, ["oal"])
    protocol: VersionRange = {"min": PROTOCOL["min"], "max": PROTOCOL["max"]}
    context = ChannelContext(
        protocol=protocol, client=client, code=code, device={"publicKey": public_key, "privateKey": private_key}
    )
    channel = await secure.open(socket, context)
    try:
        result = await exchange(
            channel,
            "host/pair",
            {"protocol": protocol, "client": client, "code": code, "device": {"name": device_name, "publicKey": public_key}},
            "the computer",
        )
    finally:
        await channel.close(1000, "")
    host, device = result["info"]["host"], result["device"]
    return Identity(
        host=HostIdentity(id=host["id"], name=host["name"], public_key=host["publicKey"]),
        device=DeviceIdentity(
            id=device["id"], name=device["name"], token=device["token"], public_key=public_key, private_key=private_key
        ),
    )


def _generate_key_pair() -> tuple[str, str]:
    """A new X25519 key pair, base64url without padding: the device's static key (spec 6.1, 17.1)."""
    key = X25519PrivateKey.generate()
    public = key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)
    private = key.private_bytes(Encoding.Raw, PrivateFormat.Raw, NoEncryption())
    return _b64url(public), _b64url(private)


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()
