from providers.provider import Provider
from scooter_position_log import ScooterPositionLog
import requests
import json
import logging



class Circ(Provider):
    name = "circ"
    _base_url = "https://node.goflash.com/devices"
    required_settings = ["circ.access_token"]

    def get_scooters(self, settings, city):
        self.check_settings(settings)

        self.refresh_token(settings)

        delta = 0.1
        params = (
            ('latitudeTopLeft', city.lat + delta),
            ('longitudeTopLeft', city.lng - delta),
            ('latitudeBottomRight', city.lat - delta),
            ('longitudeBottomRight', city.lng + delta),
        )

        headers= {
            "Authorization": settings["PROVIDERS"]["circ.access_token"]
        }

        spls = []
        r = requests.get(self._base_url, params=params, headers=headers)
        if r.status_code == 200:
            scooters = r.json()["devices"]
            for scooter in scooters:
                spls.append(ScooterPositionLog(
                    provider= self.name,
                    vehicle_id= scooter["identifier"],
                    city= city.name,
                    lat= scooter["latitude"],
                    lng= scooter["longitude"],
                    battery_level= scooter["energyLevel"],
                    raw_data=scooter
                ))
        else:
            logging.warning(f"{r.status_code} received from {self.name}, body: {r.content}")

        return spls

    def refresh_token(self, settings):

        headers = {
            'Content-type': 'application/json',
            'Accept': 'application/json',
        }

        data = { "accessToken":  settings["PROVIDERS"]["circ.access_token"],
                 "refreshToken": settings["PROVIDERS"]["circ.refresh_token"]}

        r = requests.post('https://node.goflash.com/login/refresh', headers=headers, data=json.dumps(data))

        if r.status_code == 200:
            data = r.json()
            settings["PROVIDERS"]["circ.access_token"] = data["accessToken"]
            settings["PROVIDERS"]["circ.refresh_token"] = data["refreshToken"]

            with open('settings.ini', 'w') as configfile:
                settings.write(configfile)
        else:
            logging.warning(f"{r.status_code} received from {self.name}, body: {r.content}")



