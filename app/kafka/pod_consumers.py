import logging
from app.kafka import kafka_client
from app.shared import (
    messages_pod_response,
    messages_consumed_pod_event,
    lock_pod_response,
    messages_get_all_pods_response,
    messages_consumed_get_all_pods_event,
    lock_get_all_pods_response,
    messages_get_by_id_pod_response,
    messages_consumed_get_by_id_pod_event,
    lock_get_by_id_pod_response,
    messages_update_pod_response,
    messages_consumed_update_pod_event,
    lock_update_pod_response,
    messages_delete_pod_response,
    messages_consumed_delete_pod_event,
    lock_delete_pod_response,
)


@kafka_client.topic('pod_response')
def consume_message_pod(msg):
    """
    Consumes messages from the pod_response topic.
    Handles pod creation responses and triggers related events.

    Args:
        msg: The message received from Kafka
    """
    try:
        with lock_pod_response:
            messages_pod_response.append(msg)
        logging.info(f"Consumed message in pod_response: {msg}")
        messages_consumed_pod_event.set()
    except Exception as e:
        logging.error(f"Error processing message in pod_response: {e}")


@kafka_client.topic('get_all_pods_response')
def consume_message_get_all_pods(msg):
    """
    Consumes messages from the get_all_pods_response topic.
    Handles retrieving all pods responses and triggers related events.

    Args:
        msg: The message received from Kafka
    """
    try:
        with lock_get_all_pods_response:
            messages_get_all_pods_response.append(msg)
        logging.info(f"Consumed message in get_all_pods_response: {msg}")
        messages_consumed_get_all_pods_event.set()
    except Exception as e:
        logging.error(f"Error processing message in get_all_pods_response: {e}")


@kafka_client.topic('get_by_id_pod_response')
def consume_message_get_by_id_pod(msg):
    """
    Consumes messages from the get_by_id_pod_response topic.
    Handles retrieving pod by ID responses and triggers related events.

    Args:
        msg: The message received from Kafka
    """
    try:
        with lock_get_by_id_pod_response:
            messages_get_by_id_pod_response.append(msg)
        logging.info(f"Consumed message in get_by_id_pod_response: {msg}")
        messages_consumed_get_by_id_pod_event.set()
    except Exception as e:
        logging.error(f"Error processing message in get_by_id_pod_response: {e}")


@kafka_client.topic('update_pod_response')
def consume_message_update_pod(msg):
    """
    Consumes messages from the update_pod_response topic.
    Handles pod update responses and triggers related events.

    Args:
        msg: The message received from Kafka
    """
    try:
        with lock_update_pod_response:
            messages_update_pod_response.append(msg)
        logging.info(f"Consumed message in update_pod_response: {msg}")
        messages_consumed_update_pod_event.set()
    except Exception as e:
        logging.error(f"Error processing message in update_pod_response: {e}")


@kafka_client.topic('delete_pod_response')
def consume_message_delete_pod(msg):
    """
    Consumes messages from the delete_pod_response topic.
    Handles pod deletion responses and triggers related events.

    Args:
        msg: The message received from Kafka
    """
    try:
        with lock_delete_pod_response:
            messages_delete_pod_response.append(msg)
        logging.info(f"Consumed message in delete_pod_response: {msg}")
        messages_consumed_delete_pod_event.set()
    except Exception as e:
        logging.error(f"Error processing message in delete_pod_response: {e}")
