# Test

| Heatmiser                     | Home Assistant                                       | MQTT |
|-------------------------------|------------------------------------------------------|------|
| LIVE_DATA.ACTUAL_TEMP<br>INFO.CURRENT_TEMPERATURE  | climate.<x>.current_temperature                      |      |  
| LIVE_DATA.SET_TEMP<br>INFO.CURRENT_SET_TEMPERATURE | climate.<x>.target_temperature                       |      |  
| LIVE_DATA.STANDBY<br>INFO.STANDBY                  | climate.<x>.hvac_mode = heat / off                   | |  
| LIVE_DATA.HEAT_ON<br>INFO.DEMAND                   | climate.<x>.hvac_action = heating / idle             | |  HEATING
| LIVE_DATA.ZONE_NAME<br>INFO.device                 | climate.<x>.friendly name                            | | 
|                                                    | *.<x>.available / not available                      | |  OFFLINE
|                                                    |
| ENGINEERS.FLOOR_LIMIT                              | climate.<x>_floor.target_temperature                 | |  
| CURRENT_FLOOR_TEMPERATURE                          | climate.<x>_floor.current_temperature                | |  CURRENT_FLOOR_TEMPERATURE
| LIVE_DATA.HEAT_ON<br>INFO.DEMAND                   | climate.<x>_floor.hvac_mode = heat / off             | | 
| LIVE_DATA.FLOOR_LIMIT<br>INFO.FLOOR_LIMIT         | climate.<x>_floor.hvac_action = heating / idle       | |  FLOOR_LIMIT

hvac_modes = 'off', 'heat'