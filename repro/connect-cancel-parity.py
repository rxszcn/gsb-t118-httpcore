"""四条建连路径在握手中断之后各留下什么，逐条读数。

跑法：.venv/bin/python repro/connect-cancel-parity.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import os

import anyio
import httpcore



class NeverConnects(httpcore.AsyncMockBackend):
    def __init__(self) -> None:
        super().__init__([])
        self.calls = 0

    async def connect_tcp(self, host, port, timeout=None, local_address=None,
                          socket_options=None):
        self.calls += 1
        await anyio.sleep(999)  # never completes; the caller cancels
        raise AssertionError("unreachable")  # pragma: no cover


def describe(pool) -> list:
    return [
        {
            "repr": repr(c),
            "idle": c.is_idle(),
            "closed": c.is_closed(),
            "available": c.is_available(),
            "expired": c.has_expired(),
        }
        for c in pool.connections
    ]


async def case(label: str, proxy) -> None:
    backend = NeverConnects()
    pool = httpcore.AsyncConnectionPool(
        network_backend=backend, proxy=proxy, max_connections=3
    )
    with anyio.move_on_after(0.2) as scope:
        try:
            await pool.request("GET", "https://example.com/")
        except BaseException as exc:  # noqa: BLE001
            print(f"{label}: request raised {type(exc).__name__}")
    print(f"{label}: externally cancelled={scope.cancelled_caught} "
          f"connect_tcp calls={backend.calls}")
    print(f"{label}: pool repr {repr(pool)}")
    print(f"{label}: leftover connections -> {describe(pool)}")


async def main() -> None:
    await case("direct", None)
    await case("socks5 ", httpcore.Proxy("socks5://localhost:1080/"))
    await case("tunnel ", httpcore.Proxy("http://localhost:8080/"))


anyio.run(main)
