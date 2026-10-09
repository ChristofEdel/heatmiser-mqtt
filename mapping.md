# MQTT Topic <-- Heatmiser Neohub API
```
heatmiser/
├── available                           'true', 'false' after MQTT_AVAILABILITY_TIMER_SECONDS seconds 
│   |                                   after disconnect
│   │
│   └── keepalive                       unix time_t; refreshed every 30 seconds; primarily intended to
|                                       help detecting communication issues
│
├── 3/                                  GET_LIVE_DATA.DEVICE_ID
│   ├── name                            GET_LIVE_DATA.ZONE_NAME
│   ├── online                          GET_LIVE_DATA.ONLINE
│   ├── is_timeclock                    'true'; GET_LIVE_DATA.TIMECLOCK not present ot 'false'
│   │
│   ├── thermostat/
│   │   ├── current_temperature         GET_LIVE_DATA.ACTUAL_TEMP
│   │   ├── target_temperature          GET_LIVE_DATA.SET_TEMP
│   │   ├── enabled                     GET_LIVE_DATA.STANDBY, inverted
│   │   ├── demand                      GET_LIVE_DATA.HEAT_ON
│   │   └── set/
│   │       ├── enabled                 command: FROST_OFF
│   │       ├── target_temperature      command: SET_TEMP
│   │       └── hold_temperature        command: HOLD / CANCEL_HGROUP
│   │
│   └── floor/
│       ├── current_temperature         GET_LIVE_DATA.CURRENT_FLOOR_TEMPERATURE
│       ├── limit_temperature           GET_ENGINEERS.FLOOR_LIMIT
│       ├── limit_active                GET_LIVE_DATA.FLOOR_LIMIT
│       └── set/
│           └── limit_temperature       command: SET_FLOOR
│
├── 7/                                  GET_LIVE_DATA.DEVICE_ID
│   ├── name                            GET_LIVE_DATA.ZONE_NAME
│   ├── online                          GET_LIVE_DATA.ONLINE
│   ├── is_timeclock                    'false'; GET_LIVE_DATA.TIMECLOCK present and 'true'
│   │
│   └── timeclock/
│       ├── enabled                     GET_LIVE_DATA.STANDBY, inverted
│       ├── demand                      GET_LIVE_DATA.HEAT_ON
│       └── set/
│           └── enabled                 command: FROST_OFF
```
