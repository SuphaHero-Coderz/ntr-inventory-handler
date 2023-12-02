import os
import requests
import logging as LOG
import json
import uuid
from dotenv import load_dotenv
from src.redis import RedisResource, Queue
from src.exceptions import InsufficientTokensError
import src.db_services as _services
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider

tracer = trace.get_tracer(__name__)


load_dotenv()

REDIS_QUEUE_LOCATION = os.getenv("REDIS_QUEUE", "localhost")
INVENTORY_QUEUE_NAME = os.getenv("INVENTORY_QUEUE_NAME")

QUEUE_NAME = f"queue:{INVENTORY_QUEUE_NAME}"
INSTANCE_NAME = uuid.uuid4().hex

LOG.basicConfig(
    level=LOG.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


def watch_queue(redis_conn, queue_name, callback_func, timeout=30):
    """
    Listens to queue `queue_name` and passes messages to `callback_func`
    """
    active = True

    while active:
        # Fetch a json-encoded task using a blocking (left) pop
        packed = redis_conn.blpop([queue_name], timeout=timeout)

        if not packed:
            # if nothing is returned, poll a again
            continue

        _, packed_task = packed

        # If it's treated to a poison pill, quit the loop
        if packed_task == b"DIE":
            active = False
        else:
            task = None
            try:
                task = json.loads(packed_task)
            except Exception:
                LOG.exception("json.loads failed")
                data = {"status": -1, "message": "An error occurred"}
                redis_conn.publish(INVENTORY_QUEUE_NAME, json.dumps(data))
            if task:
                callback_func(task)
                data = {"status": 1, "message": "Successfully chunked video"}
                redis_conn.publish(INVENTORY_QUEUE_NAME, json.dumps(task))


def update_order_status(order_id: int, status: str, status_message: str) -> None:
    """
    Sends a request to update order status in order service

    Args:
        order_id (int): order id to update
        status (str): status message
    """
    LOG.info(f"Updating status for order with id {order_id}: {status}")
    requests.put(
        "http://order-handler/update-order-status",
        params={
            "order_id": order_id,
            "status": status,
            "status_message": status_message,
        },
    )


def update_inventory(num_tokens: int) -> None:
    """
    Updates the inventory during an order

    Args:
        num_tokens (int): number of tokens bought

    Raises:
        InsufficientTokensError: not enough tokens in inventory
    """
    LOG.info("Updating inventory")

    num_tokens_available: int = _services.get_num_tokens()

    if num_tokens_available < num_tokens:
        raise InsufficientTokensError

    _services.deduct_tokens(num_tokens=num_tokens)


def rollback(order_id: int, user_id: int, num_tokens: int, traceparent) -> None:
    """
    Rolls back changes made

    Args:
        order_id (int): order id
        user_id (int): user id
        num_tokens (int): number of tokens
    """
    LOG.warning(f"Rolling back for order id {order_id}")
    _services.add_tokens(num_tokens=num_tokens)
    send_rollback_request(
        Queue.payment_queue,
        {"order_id": order_id, "user_id": user_id, "num_tokens": num_tokens, "traceparent": traceparent},
    )


def send_rollback_request(queue: Queue, data: dict):
    """
    Sends a notice to rollback to preceding service

    Args:
        queue (Queue): queue to send notice to
        data (dict): order information
    """
    carrier = {"traceparent": data["traceparent"]}
    ctx = TraceContextTextMapPropagator().extract(carrier)
    with tracer.start_as_current_span("rollback inventory", context=ctx):
        carrier = {}
        # pass the current context to the next service
        TraceContextTextMapPropagator().inject(carrier)
        data["traceparent"] = carrier["traceparent"]
        LOG.warning(f"Sending rollback request to {queue.value}")
        data = {"task": "rollback", **data}
        RedisResource.push_to_queue(queue, data)


def process_message(data):
    LOG.info("begin inventory")
    """
    Processes an incoming message from the work queue
    """
    try:
        if data["task"] == "rollback":
            rollback(data["order_id"], data["user_id"], data["num_tokens"], data["traceparent"])
        else:
            # get trace context from the task and create new span using the context
            carrier = {"traceparent": data["traceparent"]}
            ctx = TraceContextTextMapPropagator().extract(carrier)
            with tracer.start_as_current_span("push to delivery", context=ctx):
                user_id: int = data["user_id"]
                order_id: int = data["order_id"]
                num_tokens: int = data["num_tokens"]

                update_inventory(num_tokens)

                update_order_status(
                    order_id=order_id,
                    status="inventory",
                    status_message="Inventory updated",
                )

                LOG.info("Pushing to delivery queue")
                carrier = {}
                #pass the current context to the next service
                TraceContextTextMapPropagator().inject(carrier)
                data["traceparent"] = carrier["traceparent"]
                RedisResource.push_to_queue(Queue.delivery_queue, data)
                LOG.info("finish inventory")
    except Exception as e:
        LOG.error("ERROR OCCURED! ", e.message)
        update_order_status(
            order_id=order_id,
            status="failed",
            status_message=e.message,
        )
        send_rollback_request(
            Queue.payment_queue, {"order_id": order_id, "user_id": user_id, "num_tokens": num_tokens, "traceparent": data["traceparent"]}
        )


def main():
    LOG.info("Starting a worker...")
    LOG.info("Unique name: %s", INSTANCE_NAME)
    named_logging = LOG.getLogger(name=INSTANCE_NAME)
    named_logging.info("Trying to connect to %s", REDIS_QUEUE_LOCATION)
    named_logging.info("Listening to queue: %s", QUEUE_NAME)

    redis_conn = RedisResource.get_connection()

    watch_queue(redis_conn, QUEUE_NAME, process_message)


if __name__ == "__main__":
    _services.create_database()
    _services.populate_inventory()
    main()
