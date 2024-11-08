import logging
from RSKafkaWrapper.client import KafkaClient
from app.shared import (
    messages_pod_response,
    messages_get_all_pods_response,
    messages_get_by_id_pod_response,
    messages_consumed_pod_event,
    lock_pod_response,
    lock_get_all_pods_response,
    lock_get_by_id_pod_response,
    messages_consumed_get_by_id_pod_event, messages_consumed_update_pod_event, messages_update_pod_response,
    lock_update_pod_response, lock_delete_pod_response, messages_delete_pod_response, messages_consumed_delete_pod_event
)
from app.api.utils import parse_and_flatten_messages
from app.mapper.pod_mapper import PodMapper, UpdatePodMapper


class PodService:
    def __init__(self, client: KafkaClient):
        self.client = client

    def save_pod(self, pod_dto):
        try:
            logging.debug("Clearing previous messages and events.")
            with lock_pod_response:
                messages_pod_response.clear()
            messages_consumed_pod_event.clear()

            pod_mapper = PodMapper(
                name=pod_dto.name,
                description=pod_dto.description,
                user_id=pod_dto.user_id
            )

            logging.info(f"Sending message: {pod_mapper.model_dump()}")
            self.client.send_message("pod", pod_mapper.model_dump())

            logging.info("Waiting for message consumption event to be set.")
            messages_consumed_pod_event.wait(timeout=10)

            with lock_pod_response:
                response = parse_and_flatten_messages(messages_pod_response)
            logging.info(f"Received data SAVE: {response}")
            return response

        except Exception as e:
            logging.error(f"An error occurred while saving pod: {e}")
            raise

    def get_all_pods(self):
        try:
            logging.info("Clearing previous messages and events.")
            with lock_get_all_pods_response:
                messages_get_all_pods_response.clear()
            messages_consumed_pod_event.clear()

            to_send = {"event": "get_all"}
            logging.info(f"Sending message: {to_send}")
            self.client.send_message("get_all_pods", to_send)

            logging.info("Waiting for message consumption event to be set.")
            messages_consumed_pod_event.wait(timeout=10)

            with lock_get_all_pods_response:
                response = parse_and_flatten_messages(messages_get_all_pods_response)
            logging.info(f"Received data GET ALL: {response}")
            return response

        except Exception as e:
            logging.error(f"An error occurred while fetching all pods: {e}")
            raise

    def get_by_id_pod(self, record_id: int):
        try:
            with lock_get_by_id_pod_response:
                messages_get_by_id_pod_response.clear()
            messages_consumed_get_by_id_pod_event.clear()

            to_send = {
                "event": "get_by_id",
                "id": record_id
            }
            self.client.send_message("get_by_id_pod", to_send)

            logging.info("Waiting for message consumption event to be set.")
            messages_consumed_get_by_id_pod_event.wait(timeout=10)

            with lock_get_by_id_pod_response:
                response = parse_and_flatten_messages(messages_get_by_id_pod_response)
            logging.info(f"Received data pod: {response}")
            return response

        except Exception as e:
            logging.error(f"Error in get_by_id_pod: {e}")
            return None

    def update_pod(self, record_id, pod_dto):
        try:
            logging.debug("Clearing previous messages and events.")
            with lock_update_pod_response:
                messages_update_pod_response.clear()
            messages_consumed_update_pod_event.clear()

            update_mapper = UpdatePodMapper(id=record_id)
            update_mapper.update_from_dict(pod_dto.model_dump())

            to_send = {
                "event": "update",
                "data": update_mapper.model_dump_not_none()
            }

            logging.info(f"Sending message: {to_send}")
            self.client.send_message("update_pod", to_send)

            logging.info("Waiting for message consumption event to be set.")
            messages_consumed_update_pod_event.wait(timeout=10)

            with lock_update_pod_response:
                response = parse_and_flatten_messages(messages_update_pod_response)
            logging.info(f"Received data UPDATE: {response}")
            return response

        except Exception as e:
            logging.error(f"An error occurred while updating pod: {e}")
            raise

    def delete_pod(self, record_id: int):
        try:
            logging.debug("Clearing previous messages and events.")
            with lock_delete_pod_response:
                messages_delete_pod_response.clear()
            messages_consumed_delete_pod_event.clear()

            to_send = {
                "event": "delete",
                "id": record_id
            }

            logging.info(f"Sending message: {to_send}")
            self.client.send_message("delete_pod", to_send)

            logging.info("Waiting for message consumption event to be set.")
            messages_consumed_pod_event.wait(timeout=10)

            with lock_delete_pod_response:
                response = parse_and_flatten_messages(messages_delete_pod_response)
            logging.info(f"Received data DELETE: {response}")
            return response

        except Exception as e:
            logging.error(f"An error occurred while deleting pod: {e}")
            raise
