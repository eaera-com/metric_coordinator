import asyncio
import sys
import traceback
from nats.aio.client import Client as NATS
from nats.aio.client import Msg
from nats.errors import ConnectionClosedError, TimeoutError, NoServersError
import json
from account_metrics.configs import settings
from account_metrics.account_metric_api import AccountMetricAPI


async def nats_listen():
    nc = NATS()
    await nc.connect(settings.NATS_CONNECTION)
    print(f"Connected to NATS at {nc.connected_url.netloc}...")

    subject = f"{settings.TOPIC_PREFIX}worker.{settings.SERVER_NAME}.api.account_metric"
    sub = await nc.subscribe(subject)
    print(f"Subscribed to '{subject}'...")
    sys.stdout.flush()

    async for msg in sub.messages:
        print(f"Received a message on '{msg.subject}: {msg.data.decode()}")
        sys.stdout.flush()
        try:
            data = json.loads(msg.data.decode())
            parameters = data.get("parameters")
            method_name = data.get("request")
            print(f"Calling method {method_name} with parameters:", parameters)

            api = AccountMetricAPI()
            response = getattr(api, method_name)(**parameters)
            response = {"status": "OK"}

        except Exception as e:
            response = {"status": "Error"}
            print(traceback.format_exc())
        sys.stdout.flush()

        await nc.publish(msg.reply, json.dumps(response).encode())
