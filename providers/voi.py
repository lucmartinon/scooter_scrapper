import requests
from providers.provider import Provider, ScooterPositionLog


class Voi(Provider):
    provider = "voi"
    _base_url = "https://api.voiapp.io/v1/vehicle/status/ready"

    def get_scooters(self, city):
        params = {
            "lat": city.lat,
            "lng": city.lng
        }
        r = requests.get(self._base_url, params=params)

        scooters = r.json()
        spls = []
        for scooter in scooters:
            spls.append(ScooterPositionLog(
                provider= self.provider,
                vehicle_id= scooter["id"],
                city= city.name,
                lat= scooter["location"][1],
                lng= scooter["location"][0],
                secondary_id=scooter["short"],
                battery_level= scooter["battery"],
                raw_data=scooter
            ))
        return spls




