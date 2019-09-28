import requests
import logging
from providers.provider import Provider
from scooter_position_log import ScooterPositionLog

class Tier(Provider):
    name = "tier"
    _base_url = "https://platform.tier-services.io/vehicle"

    def get_scooters(self, settings, city):
        headers = {
            "X-Api-Key": "bpEUTJEBTf74oGRWxaIcW7aeZMzDDODe1yBoSxi2"
        }
        params = {
            "zoneId":city.tier_city_name
        }
        r = requests.get(self._base_url, headers=headers, params=params)

        scooters = r.json()["data"]
        spls = []
        if r.status_code == 200:
            for scooter in scooters:
                spls.append(ScooterPositionLog(
                    provider= self.name,
                    vehicle_id= scooter["id"],
                    city= city.name,
                    lat= scooter["lat"],
                    lng= scooter["lng"],
                    licence_plate=scooter["licencePlate"],
                    battery_level= scooter["batteryLevel"],
                    secondary_id=scooter["code"],
                    raw_data=scooter
                ))
        else:
            logging.warning(f"{r.status_code} received from {self.name}, body: {r.content}")
        return spls



