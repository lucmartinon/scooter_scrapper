import requests
import logging
import json
from providers.provider import Provider
from scooter_position_log import ScooterPositionLog


class Voi(Provider):
    name = "voi"
    _base_url = "https://api.voiapp.io/v1/"

    def get_scooters(self, settings, city):
        if not city.voi_zone_id > 0:
            logging.warning(f"No voi zone id for city: {city.name}")
            return []

        url = f"https://api.voiapp.io/v1/vehicles/zone/{int(city.voi_zone_id)}/ready"
        token = self.open_session(settings)
        headers= {
            "x-access-token": token
        }
        r = requests.get(url, headers=headers)

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

    def open_session(self, settings):
        headers = {
            'Content-type': 'application/json',
            'Accept': 'application/json',
        }

        data = {"authenticationToken": settings["PROVIDERS"]["voi.authenticationToken"]}

        r = requests.post('https://api.voiapp.io/v1/auth/session', headers=headers, data=json.dumps(data))

        if r.status_code == 200:
            data = r.json()
            token = data["accessToken"]
            settings["PROVIDERS"]["voi.authenticationToken"] = data["authenticationToken"]

            with open('settings.ini', 'w') as configfile:
                settings.write(configfile)
            return token
        else:
            logging.warning(f"{r.status_code} received from {self.name}, body: {r.content}")
