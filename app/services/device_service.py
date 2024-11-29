import logging

import httpx
from fastapi import HTTPException, status

from RSKafkaWrapper.client import KafkaClient
from app.dto.command_dto import CommandDTO
from app.dto.command_response_dto import CommandResponseDTO
from app.shared import (
    messages_device_response, messages_get_all_devices_response, messages_get_by_id_device_response,
    messages_consumed_device_event, lock_device_response, lock_get_all_devices_response,
    lock_get_by_id_device_response, messages_consumed_get_by_id_device_event,
    lock_update_device_response, messages_update_device_response,
    messages_consumed_update_device_event, lock_delete_device_response,
    messages_delete_device_response, messages_consumed_delete_device_event,
    lock_get_devices_by_pod_response, messages_get_devices_by_pod_response,
    messages_consumed_get_devices_by_pod_event,
    # New imports
    lock_register_device_response, messages_register_device_response,
    lock_device_heartbeat_response, messages_device_heartbeat_response,
    lock_device_health_check_response, messages_device_health_check_response,
    lock_device_statistics_response, messages_device_statistics_response,
    lock_esp32_status_response, messages_esp32_status_response,
    lock_device_validation_response, messages_device_validation_response,
    lock_device_sync_response, messages_device_sync_response,
    lock_device_migration_response, messages_device_migration_response,
    messages_consumed_register_device_event, messages_consumed_device_heartbeat_event,
    messages_consumed_device_health_check_event, messages_consumed_device_statistics_event,
    messages_consumed_esp32_status_event, messages_consumed_device_validation_event,
    messages_consumed_device_sync_event, messages_consumed_device_migration_event, lock_get_active_devices_response,
    messages_get_active_devices_response, messages_consumed_get_active_devices_event
)
from app.api.utils import parse_and_flatten_messages
from app.mapper.device_mapper import DeviceMapper, DeviceRegistrationMapper


class DeviceService:
    def __init__(self, client: KafkaClient):
        self.client = client

    def save_device_service(self, device_dto: dict):
        try:
            with lock_device_response:
                messages_device_response.clear()
            messages_consumed_device_event.clear()

            device_mapper = DeviceMapper(**device_dto)
            self.client.send_message("device", device_mapper.model_dump())

            messages_consumed_device_event.wait(timeout=10)

            with lock_device_response:
                return parse_and_flatten_messages(messages_device_response)
        except Exception as e:
            logging.error(f"Error in save_device_service: {e}")
            raise

    def get_all_devices_service(self):
        try:
            with lock_get_all_devices_response:
                messages_get_all_devices_response.clear()
            messages_consumed_device_event.clear()

            self.client.send_message("get_all_devices", {"event": "get_all"})

            messages_consumed_device_event.wait(timeout=10)

            with lock_get_all_devices_response:
                return parse_and_flatten_messages(messages_get_all_devices_response)
        except Exception as e:
            logging.error(f"Error in get_all_devices_service: {e}")
            raise

    def get_by_id_device_service(self, device_data: dict):
        try:
            with lock_get_by_id_device_response:
                messages_get_by_id_device_response.clear()
            messages_consumed_get_by_id_device_event.clear()

            self.client.send_message("get_by_id_device", device_data)

            messages_consumed_get_by_id_device_event.wait(timeout=10)

            with lock_get_by_id_device_response:
                return parse_and_flatten_messages(messages_get_by_id_device_response)[0]
        except Exception as e:
            logging.error(f"Error in get_by_id_device_service: {e}")
            raise

    def get_devices_by_pod_service(self, pod_data: dict):
        try:
            with lock_get_devices_by_pod_response:
                messages_get_devices_by_pod_response.clear()
            messages_consumed_get_devices_by_pod_event.clear()

            self.client.send_message("get_devices_by_pod", pod_data)

            messages_consumed_get_devices_by_pod_event.wait(timeout=10)

            with lock_get_devices_by_pod_response:
                return parse_and_flatten_messages(messages_get_devices_by_pod_response)
        except Exception as e:
            logging.error(f"Error in get_devices_by_pod_service: {e}")
            raise

    def update_device_service(self, device_data: dict):
        try:
            with lock_update_device_response:
                messages_update_device_response.clear()
            messages_consumed_update_device_event.clear()

            self.client.send_message("update_device", device_data)

            messages_consumed_update_device_event.wait(timeout=10)

            with lock_update_device_response:
                return parse_and_flatten_messages(messages_update_device_response)
        except Exception as e:
            logging.error(f"Error in update_device_service: {e}")
            raise

    def delete_device_service(self, device_data: dict):
        try:
            with lock_delete_device_response:
                messages_delete_device_response.clear()
            messages_consumed_delete_device_event.clear()

            self.client.send_message("delete_device", device_data)

            messages_consumed_delete_device_event.wait(timeout=10)

            with lock_delete_device_response:
                return parse_and_flatten_messages(messages_delete_device_response)
        except Exception as e:
            logging.error(f"Error in delete_device_service: {e}")
            raise

    def register_device_service(self, registration_data: dict):
        try:
            with lock_register_device_response:
                messages_register_device_response.clear()
            messages_consumed_register_device_event.clear()

            mapper = DeviceRegistrationMapper(**registration_data)
            self.client.send_message("create_device", mapper.model_dump())

            messages_consumed_register_device_event.wait(timeout=10)

            with lock_register_device_response:
                return parse_and_flatten_messages(messages_register_device_response)[0]
        except Exception as e:
            logging.error(f"Error in register_device_service: {e}")
            raise

    def get_device_heartbeat_service(self, heartbeat_data: dict):
        try:
            with lock_device_heartbeat_response:
                messages_device_heartbeat_response.clear()
            messages_consumed_device_heartbeat_event.clear()

            self.client.send_message("device_heartbeat", heartbeat_data)

            messages_consumed_device_heartbeat_event.wait(timeout=10)

            with lock_device_heartbeat_response:
                return parse_and_flatten_messages(messages_device_heartbeat_response)
        except Exception as e:
            logging.error(f"Error in get_device_heartbeat_service: {e}")
            raise

    def get_device_statistics_service(self, device_data: dict):
        try:
            with lock_device_statistics_response:
                messages_device_statistics_response.clear()
            messages_consumed_device_statistics_event.clear()

            self.client.send_message("device_statistics", device_data)

            messages_consumed_device_statistics_event.wait(timeout=10)

            with lock_device_statistics_response:
                return parse_and_flatten_messages(messages_device_statistics_response)
        except Exception as e:
            logging.error(f"Error in get_device_statistics_service: {e}")
            raise

    def update_esp32_status_service(self, status_data: dict):
        try:
            with lock_esp32_status_response:
                messages_esp32_status_response.clear()
            messages_consumed_esp32_status_event.clear()

            self.client.send_message("update_esp32_status", status_data)

            messages_consumed_esp32_status_event.wait(timeout=10)

            with lock_esp32_status_response:
                return parse_and_flatten_messages(messages_esp32_status_response)
        except Exception as e:
            logging.error(f"Error in update_esp32_status_service: {e}")
            raise

    def bulk_update_esp32_status_service(self, status_data: dict):
        try:
            with lock_esp32_status_response:
                messages_esp32_status_response.clear()
            messages_consumed_esp32_status_event.clear()

            self.client.send_message("bulk_update_esp32_status", status_data)

            messages_consumed_esp32_status_event.wait(timeout=10)

            with lock_esp32_status_response:
                return parse_and_flatten_messages(messages_esp32_status_response)
        except Exception as e:
            logging.error(f"Error in bulk_update_esp32_status_service: {e}")
            raise

    def validate_device_configuration_service(self, config_data: dict):
        try:
            with lock_device_validation_response:
                messages_device_validation_response.clear()
            messages_consumed_device_validation_event.clear()

            self.client.send_message("validate_device_configuration", config_data)

            messages_consumed_device_validation_event.wait(timeout=10)

            with lock_device_validation_response:
                return parse_and_flatten_messages(messages_device_validation_response)
        except Exception as e:
            logging.error(f"Error in validate_device_configuration_service: {e}")
            raise

    def sync_device_configuration_service(self, sync_data: dict):
        try:
            with lock_device_sync_response:
                messages_device_sync_response.clear()
            messages_consumed_device_sync_event.clear()

            self.client.send_message("sync_device_configuration", sync_data)

            messages_consumed_device_sync_event.wait(timeout=10)

            with lock_device_sync_response:
                return parse_and_flatten_messages(messages_device_sync_response)
        except Exception as e:
            logging.error(f"Error in sync_device_configuration_service: {e}")
            raise

    def migrate_device_service(self, migration_data: dict):
        try:
            with lock_device_migration_response:
                messages_device_migration_response.clear()
            messages_consumed_device_migration_event.clear()

            self.client.send_message("migrate_device", migration_data)

            messages_consumed_device_migration_event.wait(timeout=10)

            with lock_device_migration_response:
                return parse_and_flatten_messages(messages_device_migration_response)
        except Exception as e:
            logging.error(f"Error in migrate_device_service: {e}")
            raise

    def get_device_by_serial_service(self, serial_data: dict):
        try:
            with lock_get_by_id_device_response:  # You might want to create a specific lock for this
                messages_get_by_id_device_response.clear()
            messages_consumed_get_by_id_device_event.clear()

            self.client.send_message("get_device_by_serial", serial_data)

            messages_consumed_get_by_id_device_event.wait(timeout=10)

            with lock_get_by_id_device_response:
                return parse_and_flatten_messages(messages_get_by_id_device_response)
        except Exception as e:
            logging.error(f"Error in get_device_by_serial_service: {e}")
            raise

    def get_device_with_esp32s_service(self, device_data: dict):
        try:
            with lock_get_by_id_device_response:  # You might want to create a specific lock for this
                messages_get_by_id_device_response.clear()
            messages_consumed_get_by_id_device_event.clear()

            self.client.send_message("get_device_with_esp32s", device_data)

            messages_consumed_get_by_id_device_event.wait(timeout=10)

            with lock_get_by_id_device_response:
                return parse_and_flatten_messages(messages_get_by_id_device_response)
        except Exception as e:
            logging.error(f"Error in get_device_with_esp32s_service: {e}")
            raise

    def get_active_devices_service(self):
        try:
            with lock_get_active_devices_response:
                messages_get_active_devices_response.clear()
            messages_consumed_get_active_devices_event.clear()

            self.client.send_message("get_active_devices", {"event": "get_active"})

            messages_consumed_get_active_devices_event.wait(timeout=10)

            with lock_get_active_devices_response:
                return parse_and_flatten_messages(messages_get_active_devices_response)[0]
        except Exception as e:
            logging.error(f"Error in get_active_devices_service: {e}")
            raise

    def get_inactive_devices_service(self, threshold_data: dict):
        try:
            with lock_get_all_devices_response:
                messages_get_all_devices_response.clear()
            messages_consumed_device_event.clear()

            self.client.send_message("get_inactive_devices", threshold_data)

            messages_consumed_device_event.wait(timeout=10)

            with lock_get_all_devices_response:
                return parse_and_flatten_messages(messages_get_all_devices_response)
        except Exception as e:
            logging.error(f"Error in get_inactive_devices_service: {e}")
            raise

    def update_device_status_service(self, status_data: dict):
        try:
            with lock_update_device_response:  # You might want to create a specific lock for device status
                messages_update_device_response.clear()
            messages_consumed_update_device_event.clear()

            self.client.send_message("update_device_status", {
                "device_id": status_data["device_id"],
                "status": status_data["status"]
            })

            messages_consumed_update_device_event.wait(timeout=10)

            with lock_update_device_response:
                return parse_and_flatten_messages(messages_update_device_response)
        except Exception as e:
            logging.error(f"Error in update_device_status_service: {e}")
            raise

    def get_device_health_check_service(self, device_data: dict):
        try:
            with lock_device_health_check_response:
                messages_device_health_check_response.clear()
            messages_consumed_device_health_check_event.clear()

            self.client.send_message("device_health_check", {
                "device_id": device_data["device_id"]
            })

            messages_consumed_device_health_check_event.wait(timeout=10)

            with lock_device_health_check_response:
                return parse_and_flatten_messages(messages_device_health_check_response)
        except Exception as e:
            logging.error(f"Error in get_device_health_check_service: {e}")
            raise

    async def send_command_to_esp(
            self,
            raspberry_pi_id: int,
            esp_id: int,
            command: CommandDTO
    ) -> CommandResponseDTO:
        try:
            # Verify device exist
            raspberry_pi = self.get_by_id_device_service({"id": raspberry_pi_id})

            if not raspberry_pi:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Raspberry pi {raspberry_pi_id} not found"
                )

            logging.info(f'rasp {raspberry_pi}')

            # Find the ESP32 device
            esp_32 = next(
                (esp for esp in raspberry_pi['esp_devices'] if esp['id'] == esp_id),
                None
            )

            if esp_32 is None:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"ESP32 device with id {esp_id} not found on Raspberry Pi {raspberry_pi_id}"
                )
            # Verify ESP32 has the capability

            # Construct the Raspberry Pi URL using registered IP and port
            ip_address = raspberry_pi.get('ip_address')
            port = raspberry_pi.get('port')

            # Handle different IP address formats
            if "://" in ip_address:  # If IP includes protocol (http:// or https://)
                raspberry_pi_url = f"{ip_address}:{port}"
            else:  # If IP is just the address
                protocol = "https" if port == 443 else "http"
                raspberry_pi_url = f"{protocol}://{ip_address}:{port}"

            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{raspberry_pi_url}/esp32/{esp_id}/command",
                    json=command.model_dump(),
                    timeout=float(command.timeout)
                )

                if response.status_code != 200:
                    logging.error(f"Command failed: {response.text}")
                    raise HTTPException(
                        status_code=response.status_code,
                        detail="Command execution failed"
                    )

                return CommandResponseDTO(
                    success=True,
                    message="Command executed successfully",
                    data=response.json()
                )

        except httpx.RequestError as e:
            logging.error(f"Error sending command: {e}")
            raise HTTPException(status_code=503, detail="Failed to communicate with Raspberry Pi")

        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            raise HTTPException(status_code=500, detail=str(e))
