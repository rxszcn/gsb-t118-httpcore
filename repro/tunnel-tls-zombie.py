"""隧道代理握手中断后池里剩几条连接，六行读数。

跑法：.venv/bin/python repro/tunnel-tls-zombie.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import os

import anyio
import httpcore


CONNECT_OK = b"HTTP/1.1 200 OK\r\n\r\n"
SERVER_CLOSES = b"HTTP/1.1 502 Bad Gateway\r\n\r\n"


class TlsFailingStream(httpcore.AsyncMockStream):
    async def start_tls(self, ssl_context, server_hostname=None, timeout=None):
        raise httpcore.ConnectError("TLS handshake blew up")


class TlsFailingBackend(httpcore.AsyncMockBackend):
    async def connect_tcp(self, host, port, timeout=None, local_address=None,
                          socket_options=None):
        return TlsFailingStream([CONNECT_OK, SERVER_CLOSES])


def unwrap(conn):
    """Tunnel connections delegate to an AsyncHTTPConnection wrapper for the proxy
    leg; walk down until we reach an object that owns a network stream."""
    while not hasattr(conn, "_network_stream") and getattr(conn, "_connection", None):
        conn = conn._connection
    return conn


def state(pool):
    out = []
    for wrapper in pool.connections:
        inner = unwrap(wrapper._connection)
        out.append({
            "repr": repr(wrapper),
            "inner": repr(inner),
            "idle": wrapper.is_idle(),
            "closed": wrapper.is_closed(),
            "available": wrapper.is_available(),
            "expired": wrapper.has_expired(),
            "socket_closed": getattr(inner._network_stream, "_closed", None),
        })
    return out


async def main() -> None:
    pool = httpcore.AsyncConnectionPool(
        proxy=httpcore.Proxy("http://localhost:8080/"),
        network_backend=TlsFailingBackend([]),
        max_connections=3,
    )
    async with pool:
        for attempt in range(1, 5):
            try:
                await pool.request(
                    "GET", "https://example.com/",
                    extensions={"timeout": {"pool": 0.3}},
                )
            except BaseException as exc:  # noqa: BLE001
                outcome = f"{type(exc).__name__}: {exc}"
            else:
                outcome = "status=?"
            print(f"attempt {attempt}: {outcome} | connections: "
                  f"{len(pool.connections)} | {repr(pool)}")

        print("zombie detail:", state(pool))

        # With the slots consumed, an unrelated origin can no longer be reached.
        try:
            await pool.request(
                "GET", "https://other.com/", extensions={"timeout": {"pool": 0.2}}
            )
        except BaseException as exc:  # noqa: BLE001
            print("other origin now:", type(exc).__name__, exc)


anyio.run(main)
