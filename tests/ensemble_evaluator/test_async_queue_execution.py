import asyncio
import threading

import pytest
import websockets
from cloudevents.http import from_json
from http import HTTPStatus
from ert_shared.ensemble_evaluator.utils import wait_for_evaluator


async def mock_ws(host, port, done):
    events = []

    async def process_request(path, request_headers):
        if path == "/healthcheck":
            return HTTPStatus.OK, {}, b""

    async def _handler(websocket, path):
        while True:
            event = await websocket.recv()
            events.append(event)
            cloud_event = from_json(event)
            if cloud_event["type"] == "com.equinor.ert.forward_model_stage.success":
                break

    async with websockets.serve(_handler, host, port, process_request=process_request):
        await done
    return events


@pytest.mark.asyncio
@pytest.mark.timeout(60)
async def test_happy_path(
    tmpdir, unused_tcp_port, event_loop, make_ensemble_builder, queue_config, caplog
):
    asyncio.set_event_loop(event_loop)
    host = "localhost"
    url = f"ws://{host}:{unused_tcp_port}"

    done = asyncio.get_event_loop().create_future()
    mock_ws_task = asyncio.get_event_loop().create_task(
        mock_ws(host, unused_tcp_port, done)
    )
    await wait_for_evaluator(base_url=url, timeout=5)

    ensemble = make_ensemble_builder(tmpdir, 1, 1).build()
    queue = queue_config.create_job_queue()
    for real in ensemble.get_reals():
        queue.add_ee_stage(real.get_steps()[0], None)
    queue.submit_complete()

    await queue.execute_queue_async(
        url, "ee_0", threading.BoundedSemaphore(value=10), None
    )
    done.set_result(None)

    await mock_ws_task

    mock_ws_task.result()

    assert mock_ws_task.done()

    event_0 = from_json(mock_ws_task.result()[0])
    assert event_0["source"] == "/ert/ee/ee_0/real/0/step/0"
    assert event_0["type"] == "com.equinor.ert.forward_model_step.waiting"
    assert event_0.data == {"queue_event_type": "JOB_QUEUE_WAITING"}

    end_event_index = len(mock_ws_task.result()) - 1
    end_event = from_json(mock_ws_task.result()[end_event_index])
    assert end_event["type"] == "com.equinor.ert.forward_model_step.success"
    assert end_event.data == {"queue_event_type": "JOB_QUEUE_SUCCESS"}
