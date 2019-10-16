import requests
import logging
from providers.provider import Provider
from scooter_position_log import ScooterPositionLog


class Voi(Provider):
    name = "voi"
    _base_url = "https://api.voiapp.io/v1/vehicle/status/ready"

    def get_scooters(self, settings, city):
        params = {
            "lat": city.lat,
            "lng": city.lng
        }
        r = requests.get(self._base_url, params=params)

        scooters = r.json()
        spls = []
        if r.status_code == 200:
            for scooter in scooters:
                spls.append(ScooterPositionLog(
                    provider= self.name,
                    vehicle_id= scooter["id"],
                    city= city.name,
                    lat= scooter["location"][0],
                    lng= scooter["location"][1],
                    secondary_id=scooter["short"],
                    battery_level= scooter["battery"],
                    raw_data=scooter
                ))
        else:
            logging.warning(f"{r.status_code} received from {self.name}, body: {r.content}")
        return spls




