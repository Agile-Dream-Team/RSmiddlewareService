import logging
from fastapi import APIRouter, Depends, HTTPException

from app.dto.command_dto import CommandDTO
from app.dto.command_response_dto import CommandResponseDTO
from app.dto.device_dto import DeviceDTO, DeviceRegistrationDTO, ESP32StatusUpdateDTO
from app.responses.custom_responses import ErrorModel
from app.services.device_service import DeviceService
from RSKafkaWrapper.client import KafkaClient
from typing import Optional

device_router = APIRouter()

response_models = {
    400: {"model": ErrorModel},
    401: {"model": ErrorModel},
    500: {"model": ErrorModel}
}


def get_kafka_client() -> KafkaClient:
    return KafkaClient.instance()


def get_device_service(client: KafkaClient = Depends(get_kafka_client)) -> DeviceService:
    return DeviceService(client)


@device_router.post("/register")
async def register_device(device_data: DeviceRegistrationDTO, service: DeviceService = Depends(get_device_service)):
    received_data = service.register_device_service(device_data.model_dump())
    logging.info(f"Received data: {received_data}")

    status_code = received_data.get('status_code', 500)
    if status_code != 200:
        error_message = received_data.get('error', 'Unknown error occurred')
        logging.error(f"Error in register_device: {error_message}")
        raise HTTPException(
            status_code=status_code,
            detail={
                "status_code": status_code,
                "error": error_message
            }
        )

    return received_data


@device_router.get("/active")
async def get_active_devices(service: DeviceService = Depends(get_device_service)):
    return service.get_active_devices_service()


"""
@device_router.get("/inactive")
async def get_inactive_devices(threshold_minutes: Optional[int] = 5,
                               service: DeviceService = Depends(get_device_service)):
    return service.get_inactive_devices_service({"threshold_minutes": threshold_minutes})
"""

@device_router.get("/{record_id}")
async def get_device_by_id(record_id: int, service: DeviceService = Depends(get_device_service)):
    return service.get_by_id_device_service({"id": record_id})

"""
@device_router.get("/serial/{serial}")
async def get_device_by_serial(serial: str, service: DeviceService = Depends(get_device_service)):
    return service.get_device_by_serial_service({"serial": serial})
"""

@device_router.get("/pod/{pod_id}")
async def get_devices_by_pod(pod_id: int, service: DeviceService = Depends(get_device_service)):
    return service.get_devices_by_pod_service({"pod_id": pod_id})


@device_router.get("/{device_id}/esp32s")
async def get_device_with_esp32s(device_id: int, service: DeviceService = Depends(get_device_service)):
    return service.get_device_with_esp32s_service({"device_id": device_id})


@device_router.patch("/{record_id}")
async def update_device(record_id: int, device_data: DeviceDTO,
                        service: DeviceService = Depends(get_device_service)):
    device_data_dict = device_data.model_dump()
    device_data_dict["id"] = record_id
    return service.update_device_service({"data": device_data_dict})


"""
@device_router.patch("/{device_id}/status")
async def update_device_status(device_id: int, status: str,
                               service: DeviceService = Depends(get_device_service)):
    return service.update_device_status_service({
        "device_id": device_id,
        "status": status
    })


@device_router.patch("/esp32/status")
async def update_esp32_status(status_update: ESP32StatusUpdateDTO,
                              service: DeviceService = Depends(get_device_service)):
    return service.update_esp32_status_service(status_update.model_dump())


@device_router.post("/esp32/bulk-status")
async def bulk_update_esp32_status(updates: list[ESP32StatusUpdateDTO],
                                   service: DeviceService = Depends(get_device_service)):
    return service.bulk_update_esp32_status_service({"updates": [u.model_dump() for u in updates]})

"""


@device_router.delete("/{record_id}")
async def delete_device(record_id: int, service: DeviceService = Depends(get_device_service)):
    return service.delete_device_service({"id": record_id})


"""
@device_router.post("/{device_id}/heartbeat")
async def device_heartbeat(device_id: int, status: Optional[str] = "active",
                           service: DeviceService = Depends(get_device_service)):
    return service.get_device_heartbeat_service({
        "device_id": device_id,
        "status": status
    })


@device_router.get("/{device_id}/statistics")
async def get_device_statistics(device_id: int, service: DeviceService = Depends(get_device_service)):
    return service.get_device_statistics_service({"device_id": device_id})


@device_router.get("/{device_id}/health")
async def get_device_health_check(device_id: int, service: DeviceService = Depends(get_device_service)):
    return service.get_device_health_check_service({"device_id": device_id})


@device_router.post("/{device_id}/validate")
async def validate_device_configuration(device_id: int,
                                        service: DeviceService = Depends(get_device_service)):
    return service.validate_device_configuration_service({"device_id": device_id})


@device_router.post("/{device_id}/sync")
async def sync_device_configuration(device_id: int, configuration: dict,
                                    service: DeviceService = Depends(get_device_service)):
    return service.sync_device_configuration_service({
        "device_id": device_id,
        "configuration": configuration
    })


@device_router.post("/{device_id}/migrate/{new_pod_id}")
async def migrate_device(device_id: int, new_pod_id: int,
                         service: DeviceService = Depends(get_device_service)):
    return service.migrate_device_service({
        "device_id": device_id,
        "new_pod_id": new_pod_id
    })
"""


@device_router.post("/{raspberry_pi_id}/{esp_id}", response_model=CommandResponseDTO)
async def send_command(
        raspberry_pi_id: str,
        esp_id: str,
        command: CommandDTO,
        service: DeviceService = Depends(get_device_service)
):
    """Send a command to a specific ESP32 through its Raspberry Pi"""
    logging.info(f"Sending command to ESP32 {esp_id} via Raspberry Pi {raspberry_pi_id}")
    return await service.send_command_to_esp(raspberry_pi_id, esp_id, command)
