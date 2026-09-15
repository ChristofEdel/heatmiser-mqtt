# MQTT Topic <-- Heatmiser Neohub API
```
heatmiser/
├── availability                        'online', 'offline' MQTT_AVAILABILITY_TIMER_SECONDS seconds
│                                       after disconnect
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
│   │       ├── enabled                 command: ???
│   │       └── target_temperature      command: ???
│   │
│   └── floor/
│       ├── current_temperature         GET_LIVE_DATA.CURRENT_FLOOR_TEMPERATURE
│       ├── limit_temperature           GET_ENGINEERS.FLOOR_LIMIT
│       ├── limit_active                GET_LIVE_DATA.FLOOR_LIMIT
│       └── set/
│           └── limit_temperature       command: ???
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
│           └── enabled                 command: ???
```
