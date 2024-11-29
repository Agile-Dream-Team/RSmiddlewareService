import logging
from fastapi import APIRouter, Depends, HTTPException
from typing import Optional
from datetime import datetime
import httpx

from app.api.v1.enums import DeviceStatus, DeviceType
from app.dto.command_dto import CommandDTO
from app.dto.command_response_dto import CommandResponseDTO
from app.dto.esp32_dto import ESP32DTO
from app.dto.raspberry_pi_registration_dto import RaspberryPiRegistrationDTO

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Device Registry Implementation
class DeviceRegistry:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DeviceRegistry, cls).__new__(cls)
            cls._instance.raspberry_pis = {}
            cls._instance.esp_devices = {}
        return cls._instance

    def register_raspberry_pi(self, device_data: RaspberryPiRegistrationDTO) -> dict:
        try:
            raspberry_pi = {
                **device_data.model_dump(exclude={'esp_devices'}),
                "type": DeviceType.RASPBERRY_PI,
                "last_seen": datetime.now(),
                "status": DeviceStatus.ACTIVE,
                "connected_esp32s": []
            }

            self.raspberry_pis[device_data.device_id] = raspberry_pi

            for esp in device_data.esp_devices:
                self.register_esp32(device_data.device_id, esp)

            logger.info(
                f"Registered Raspberry Pi: {device_data.device_id} with {len(device_data.esp_devices)} ESP32 devices")
            return raspberry_pi

        except Exception as e:
            logger.error(f"Error registering Raspberry Pi: {e}")
            raise HTTPException(status_code=400, detail=str(e))

    def register_esp32(self, raspberry_pi_id: str, esp_data: ESP32DTO) -> dict:
        if raspberry_pi_id not in self.raspberry_pis:
            raise HTTPException(status_code=404, detail="Raspberry Pi not found")

        esp_device = {
            **esp_data.model_dump(),
            "type": DeviceType.ESP32,
            "parent_pi": raspberry_pi_id,
            "last_seen": datetime.now(),
            "status": DeviceStatus.ACTIVE
        }

        self.esp_devices[esp_data.esp_id] = esp_device
        self.raspberry_pis[raspberry_pi_id]["connected_esp32s"].append(esp_data.esp_id)

        logger.info(f"Registered ESP32: {esp_data.esp_id} under Raspberry Pi: {raspberry_pi_id}")
        return esp_device

    async def send_command_to_esp(
            self,
            raspberry_pi_id: str,
            esp_id: str,
            command: CommandDTO
    ) -> CommandResponseDTO:
        try:
            # Verify devices exist
            raspberry_pi = self.raspberry_pis.get(raspberry_pi_id)
            if not raspberry_pi:
                raise HTTPException(status_code=404, detail=f"Raspberry Pi {raspberry_pi_id} not found")

            esp_device = self.esp_devices.get(esp_id)
            if not esp_device:
                raise HTTPException(status_code=404, detail=f"ESP32 {esp_id} not found")

            # Verify ESP32 belongs to this Raspberry Pi
            if esp_device["parent_pi"] != raspberry_pi_id:
                raise HTTPException(status_code=400, detail="ESP32 does not belong to this Raspberry Pi")

            # Verify ESP32 has the capability
            command_capability = command.command_type.value.split('_')[0].lower()
            if command_capability not in [cap.value for cap in esp_device["capabilities"]]:
                raise HTTPException(
                    status_code=400,
                    detail=f"ESP32 does not have {command_capability} capability"
                )

            # Construct the Raspberry Pi URL using registered IP and port
            ip_address = raspberry_pi["ip_address"]
            port = raspberry_pi["port"]

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
                    logger.error(f"Command failed: {response.text}")
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
            logger.error(f"Error sending command: {e}")
            raise HTTPException(status_code=503, detail="Failed to communicate with Raspberry Pi")

        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    def update_device_status(
            self,
            device_id: str,
            status: DeviceStatus,
            device_type: DeviceType
    ) -> bool:
        device_store = self.raspberry_pis if device_type == DeviceType.RASPBERRY_PI else self.esp_devices

        if device_id in device_store:
            device_store[device_id].update({
                "status": status,
                "last_seen": datetime.now()
            })
            return True
        return False

    def get_raspberry_pi(self, device_id: str) -> Optional[dict]:
        pi = self.raspberry_pis.get(device_id)
        if pi:
            pi['esp_devices'] = [
                self.esp_devices[esp_id]
                for esp_id in pi['connected_esp32s']
                if esp_id in self.esp_devices
            ]
        return pi

    def get_esp32(self, esp_id: str) -> Optional[dict]:
        return self.esp_devices.get(esp_id)

    def get_all_devices(self) -> dict:
        result = {}
        for pi_id, pi_data in self.raspberry_pis.items():
            result[pi_id] = {
                **pi_data,
                'esp_devices': [
                    self.esp_devices[esp_id]
                    for esp_id in pi_data['connected_esp32s']
                    if esp_id in self.esp_devices
                ]
            }
        return result

    def remove_device(self, device_id: str, device_type: DeviceType) -> bool:
        try:
            if device_type == DeviceType.RASPBERRY_PI:
                if device_id in self.raspberry_pis:
                    # Remove all associated ESP32 devices
                    for esp_id in self.raspberry_pis[device_id]["connected_esp32s"]:
                        self.esp_devices.pop(esp_id, None)
                    self.raspberry_pis.pop(device_id)
                    logger.info(f"Removed Raspberry Pi: {device_id} and its ESP32 devices")
                    return True
            else:
                if device_id in self.esp_devices:
                    # Remove ESP32 from its parent Raspberry Pi
                    parent_pi = self.esp_devices[device_id]["parent_pi"]
                    if parent_pi in self.raspberry_pis:
                        self.raspberry_pis[parent_pi]["connected_esp32s"].remove(device_id)
                    self.esp_devices.pop(device_id)
                    logger.info(f"Removed ESP32: {device_id}")
                    return True
            return False
        except Exception as e:
            logger.error(f"Error removing device: {e}")
            return False


# Router implementation
command_router = APIRouter()


# Dependencies


# Dependencies
def get_device_registry() -> DeviceRegistry:
    return DeviceRegistry()


# Device Registration Endpoints
@command_router.post("/raspberry-pi/register", response_model=dict)
async def register_raspberry_pi(
        device_data: RaspberryPiRegistrationDTO,
        registry: DeviceRegistry = Depends(get_device_registry)
):
    """Register a Raspberry Pi with its ESP32 devices"""
    logger.info(f"Registering Raspberry Pi: {device_data.device_id}")
    return registry.register_raspberry_pi(device_data)


@command_router.post("/raspberry-pi/{pi_id}/esp32", response_model=dict)
async def add_esp32_to_raspberry_pi(
        pi_id: str,
        esp_data: ESP32DTO,
        registry: DeviceRegistry = Depends(get_device_registry)
):
    """Add a new ESP32 to an existing Raspberry Pi"""
    logger.info(f"Adding ESP32 {esp_data.esp_id} to Raspberry Pi: {pi_id}")
    return registry.register_esp32(pi_id, esp_data)


# Device Management Endpoints
@command_router.get("/devices", response_model=dict)
async def get_all_devices(
        registry: DeviceRegistry = Depends(get_device_registry)
):
    """Get all registered devices"""
    logger.info("Fetching all devices")
    return registry.get_all_devices()


@command_router.get("/raspberry-pi/{pi_id}", response_model=Optional[dict])
async def get_raspberry_pi(
        pi_id: str,
        registry: DeviceRegistry = Depends(get_device_registry)
):
    """Get details of a specific Raspberry Pi"""
    logger.info(f"Fetching Raspberry Pi: {pi_id}")
    device = registry.get_raspberry_pi(pi_id)
    if not device:
        raise HTTPException(status_code=404, detail="Raspberry Pi not found")
    return device


@command_router.get("/esp32/{esp_id}", response_model=Optional[dict])
async def get_esp32(
        esp_id: str,
        registry: DeviceRegistry = Depends(get_device_registry)
):
    """Get details of a specific ESP32"""
    logger.info(f"Fetching ESP32: {esp_id}")
    device = registry.get_esp32(esp_id)
    if not device:
        raise HTTPException(status_code=404, detail="ESP32 not found")
    return device


# Device Status Endpoints
@command_router.post("/devices/{device_id}/heartbeat")
async def device_heartbeat(
        device_id: str,
        device_type: DeviceType,
        status: DeviceStatus = DeviceStatus.ACTIVE,
        registry: DeviceRegistry = Depends(get_device_registry)
):
    """Update device heartbeat and status"""
    logger.info(f"Updating heartbeat for {device_type} {device_id}")
    if registry.update_device_status(device_id, status, device_type):
        return {"status": "success", "message": "Heartbeat updated"}
    raise HTTPException(status_code=404, detail="Device not found")


# Command Endpoints
@command_router.post("/command/{raspberry_pi_id}/{esp_id}", response_model=CommandResponseDTO)
async def send_command(
        raspberry_pi_id: str,
        esp_id: str,
        command: CommandDTO,
        registry: DeviceRegistry = Depends(get_device_registry)
):
    """Send a command to a specific ESP32 through its Raspberry Pi"""
    logger.info(f"Sending command to ESP32 {esp_id} via Raspberry Pi {raspberry_pi_id}")
    return await registry.send_command_to_esp(raspberry_pi_id, esp_id, command)


# Device Removal Endpoints
@command_router.delete("/devices/{device_id}")
async def remove_device(
        device_id: str,
        device_type: DeviceType,
        registry: DeviceRegistry = Depends(get_device_registry)
):
    """Remove a device (Raspberry Pi or ESP32)"""
    logger.info(f"Removing {device_type} {device_id}")
    if registry.remove_device(device_id, device_type):
        return {"status": "success", "message": f"{device_type} removed successfully"}
    raise HTTPException(status_code=404, detail="Device not found")
