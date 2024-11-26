import logging
from app.kafka import kafka_client
from app.shared import (
    messages_prediction_response,
    messages_consumed_prediction_event,
    lock_prediction_response,
)


@kafka_client.topic('prediction_response')
def consume_message_prediction(msg):
    """
    Consumes messages from the prediction_response topic.
    Handles prediction responses and triggers related events.

    Args:
        msg: The message received from Kafka

    The function:
    1. Appends the prediction message to the shared messages list
    2. Sets the event flag to indicate message consumption
    3. Logs the operation for monitoring
    """
    try:
        with lock_prediction_response:
            messages_prediction_response.append(msg)
        logging.info(f"Consumed message in prediction_response: {msg}")
        messages_consumed_prediction_event.set()
    except Exception as e:
        logging.error(f"Error processing message in prediction_response: {e}")
