import logging

REQUEST_INTERVAL_SECONDS = 10
REQUEST_TIMEOUT_SECONDS = 10

MAX_OPEN_RETRIES = 5
OPEN_RETRY_DELAY_SECONDS = 2

MAX_CONSECUTIVE_REQUEST_FAILURES = 5

LOGGER = logging.getLogger(__name__)

NEOHUB_HOST = "192.168.1.139"


MQTT_ROOT = "heatmiser"

# Retained state will expire from the broker after this many seconds.
# Unchanged values are automatically refreshed before this happens.
#
# Set to None for indefinite retention and strict "publish only on change".
MQTT_RETAINED_EXPIRY_SECONDS: int | None = 24 * 60 * 60

# Republish retained data when this fraction of its expiry has elapsed.
MQTT_REFRESH_FRACTION = 0.80
MQTT_CLIENT_NAME = 'heatmiser-mqtt'
MQTT_HOST = "mqtt.internal"
MQTT_PORT = 1883
MQTT_CONNECT_RETRY_SECONDS = 5
MQTT_AVAILABILITY_TIMER_SECONDS = 60

MQTT_DISCOVERY_PREFIX = "homeassistant"