import logging

from environment import required_env, env_int, env_float, env_str, env_optional_int


# Neohuub connection configuratiojn

NEOHUB_HOST                      = required_env("NEOHUB_HOST")

REQUEST_TIMEOUT_SECONDS          = env_int("REQUEST_TIMEOUT_SECONDS", 10)
MAX_OPEN_RETRIES                 = env_int("MAX_OPEN_RETRIES", 5)
OPEN_RETRY_DELAY_SECONDS         = env_int("OPEN_RETRY_DELAY_SECONDS", 2)
MAX_CONSECUTIVE_REQUEST_FAILURES = env_int("MAX_CONSECUTIVE_REQUEST_FAILURES", 5)


# MQTT connection configuration
MQTT_HOST                       = required_env("MQTT_HOST")
MQTT_PORT                       = env_int("MQTT_PORT", 1883)

MQTT_CLIENT_NAME                = required_env("MQTT_CLIENT_NAME")

MQTT_ROOT                       = required_env("MQTT_ROOT")

MQTT_CONNECT_RETRY_SECONDS      = env_int("MQTT_CONNECT_RETRY_SECONDS", 5)


# MQTT Message retention configuration
#
# Retained messages state will expire from the broker after MQTT_RETAINED_EXPIRY_SECONDS.
# Unchanged values are automatically refreshed after MQTT_REFRESH_FRACTION of this
# interval hase expired
#
# Set MQTT_RETAINED_EXPIRY_SECONDS to "none" for indefinite retention and
# strict "publish only on change".
MQTT_RETAINED_EXPIRY_SECONDS     = env_optional_int("MQTT_RETAINED_EXPIRY_SECONDS",  24 * 60 * 60)
MQTT_REFRESH_FRACTION            = env_float("MQTT_REFRESH_FRACTION", 0.8)
MQTT_AVAILABILITY_TIMER_SECONDS  = env_int("MQTT_AVAILABILITY_TIMER_SECONDS", 50)
MQTT_KEEPALIVE_INTERVAL_SECONDS  = env_int("MQTT_KEEPALIVE_INTERVAL_SECONDS", 10)

# Homeassistant autodiscovery configuration
DEFAULT_MQTT_DISCOVERY_PREFIX   = "homeassistant"
MQTT_DISCOVERY_PREFIX           = env_str("MQTT_DISCOVERY_PREFIX", "homeassistant")


LOGGER = logging.getLogger(__name__)
