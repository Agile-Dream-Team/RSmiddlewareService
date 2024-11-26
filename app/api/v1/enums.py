from enum import Enum


class DeviceType(str, Enum):
    RASPBERRY_PI = "raspberry_pi"
    ESP32 = "esp32"


class DeviceCapability(str, Enum):
    TEMPERATURE = "temperature"
    HUMIDITY = "humidity"
    RELAY = "relay"
    LED = "led"
    PUMP = "pump"
    VALVE = "valve"


class CommandType(str, Enum):
    RELAY_CONTROL = "relay_control"
    LED_CONTROL = "led_control"
    PUMP_CONTROL = "pump_control"
    VALVE_CONTROL = "valve_control"
    SENSOR_READ = "sensor_read"


class DeviceStatus(str, Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    MAINTENANCE = "maintenance"
    ERROR = "error"
