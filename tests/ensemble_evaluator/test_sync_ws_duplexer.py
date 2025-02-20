import asyncio
from contextlib import ExitStack
from http import HTTPStatus
from threading import Thread
from unittest.mock import patch

import pytest
import websockets
from ert_shared.ensemble_evaluator.config import EvaluatorServerConfig
from ert_shared.ensemble_evaluator.sync_ws_duplexer import SyncWebsocketDuplexer
from websockets.exceptions import ConnectionClosedOK


@pytest.fixture
def ws(event_loop: asyncio.AbstractEventLoop):
    t = Thread(target=event_loop.run_forever)
    t.start()

    async def _process_request(path, request_headers):
        if path == "/healthcheck":
            return HTTPStatus.OK, {}, b""

    def _start_ws(host: str, port: int, handler, ssl=None, sock=None):
        kwargs = {
            "process_request": _process_request,
            "ssl": ssl,
        }
        if sock:
            kwargs["sock"] = sock
        else:
            kwargs["host"] = host
            kwargs["port"] = port
        event_loop.call_soon_threadsafe(
            asyncio.ensure_future,
            websockets.serve(
                handler,
                **kwargs,
            ),
        )

    yield _start_ws
    event_loop.call_soon_threadsafe(event_loop.stop)
    t.join()


def test_immediate_stop():
    duplexer = SyncWebsocketDuplexer("ws://localhost", "", None, None)
    duplexer.stop()


def test_failed_connection():
    with patch(
        "ert_shared.ensemble_evaluator.sync_ws_duplexer.wait_for_evaluator"
    ) as w:
        w.side_effect = OSError("expected oserror")
        duplexer = SyncWebsocketDuplexer(
            "ws://localhost:0", "http://localhost:0", None, None
        )
        with pytest.raises(OSError):
            duplexer.send("hello")


def test_unexpected_close(unused_tcp_port, ws):
    async def handler(websocket, path):
        await websocket.close()

    ws("localhost", unused_tcp_port, handler)
    with ExitStack() as stack:
        duplexer = SyncWebsocketDuplexer(
            f"ws://localhost:{unused_tcp_port}",
            f"ws://localhost:{unused_tcp_port}",
            None,
            None,
        )
        stack.callback(duplexer.stop)
        with pytest.raises(ConnectionClosedOK):
            next(duplexer.receive())


def test_receive(unused_tcp_port, ws):
    async def handler(websocket, path):
        await websocket.send("Hello World")

    ws("localhost", unused_tcp_port, handler)
    with ExitStack() as stack:
        duplexer = SyncWebsocketDuplexer(
            f"ws://localhost:{unused_tcp_port}",
            f"ws://localhost:{unused_tcp_port}",
            None,
            None,
        )
        stack.callback(duplexer.stop)
        assert next(duplexer.receive()) == "Hello World"


def test_echo(unused_tcp_port, ws):
    async def handler(websocket, path):
        msg = await websocket.recv()
        await websocket.send(msg)

    ws("localhost", unused_tcp_port, handler)
    with ExitStack() as stack:
        duplexer = SyncWebsocketDuplexer(
            f"ws://localhost:{unused_tcp_port}",
            f"ws://localhost:{unused_tcp_port}",
            None,
            None,
        )
        stack.callback(duplexer.stop)

        duplexer.send("Hello World")
        assert next(duplexer.receive()) == "Hello World"


def test_generator(unused_tcp_port, ws):
    async def handler(websocket, path):
        await websocket.send("one")
        await websocket.send("two")
        await websocket.send("three")
        await websocket.send("four")

    ws("localhost", unused_tcp_port, handler)
    with ExitStack() as stack:
        duplexer = SyncWebsocketDuplexer(
            f"ws://localhost:{unused_tcp_port}",
            f"ws://localhost:{unused_tcp_port}",
            None,
            None,
        )
        stack.callback(duplexer.stop)

        expected = ["one", "two", "three"]
        for msg in duplexer.receive():
            assert msg == expected.pop(0)

            # Cause a GeneratorExit
            if len(expected) == 1:
                break


def test_secure_echo(unused_tcp_port, ws):
    config = EvaluatorServerConfig(unused_tcp_port)

    async def handler(websocket, path):
        msg = await websocket.recv()
        await websocket.send(msg)

    ws(
        config.host,
        unused_tcp_port,
        handler,
        ssl=config.get_server_ssl_context(),
        sock=config.get_socket(),
    )
    with ExitStack() as stack:
        duplexer = SyncWebsocketDuplexer(
            f"wss://{config.host}:{unused_tcp_port}",
            f"wss://{config.host}:{unused_tcp_port}",
            cert=config.cert,
            token=None,
        )
        stack.callback(duplexer.stop)
        duplexer.send("Hello Secure World")
        assert next(duplexer.receive()) == "Hello Secure World"
