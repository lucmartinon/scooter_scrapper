import requests
from providers.provider import Provider, ScooterPositionLog

class Tier(Provider):
    provider = "tier"
    _base_url = "https://platform.tier-services.io/vehicle"

    def get_scooters(self, city):
        headers = {
            "X-Api-Key": "bpEUTJEBTf74oGRWxaIcW7aeZMzDDODe1yBoSxi2"
        }
        params = {
            "zoneId":city.tier_city_name
        }
        r = requests.get(self._base_url, headers=headers, params=params)

        scooters = r.json()["data"]
        spls = []
        for scooter in scooters:
            spls.append(ScooterPositionLog(
                provider= self.provider,
                vehicle_id= scooter["id"],
                city= city.name,
                lat= scooter["lat"],
                lng= scooter["lng"],
                licence_plate=scooter["licencePlate"],
                battery_level= scooter["batteryLevel"],
                secondary_id=scooter["code"],
                raw_data=scooter
            ))
        return spls



