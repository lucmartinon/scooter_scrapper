from providers.provider import Provider, ScooterPositionLog
import requests
import json
import logging



class Circ(Provider):
    provider = "circ"
    _base_url = "https://node.goflash.com/devices"
    required_settings = ["circ.access_token"]

    def get_scooters(self, city):
        self.refresh_token()

        delta = 0.1
        params = (
            ('latitudeTopLeft', city.lat + delta),
            ('longitudeTopLeft', city.lng - delta),
            ('latitudeBottomRight', city.lat - delta),
            ('longitudeBottomRight', city.lng + delta),
        )

        headers= {
          "Authorization": self.settings.defaults()["circ.access_token"]
        }

        r = requests.get(self._base_url, params=params, headers=headers)
        if r.status_code == 200:
            scooters = r.json()["devices"]
            spls = []
            for scooter in scooters:
                spls.append(ScooterPositionLog(
                    provider= self.provider,
                    vehicle_id= scooter["identifier"],
                    city= city.name,
                    lat= scooter["latitude"],
                    lng= scooter["longitude"],
                    battery_level= scooter["energyLevel"],
                    raw_data=scooter
                ))
            return spls

        elif r.status_code == 401:
            logging.warning("circ scrapping failed")

    def refresh_token(self):

        headers = {
            'Content-type': 'application/json',
            'Accept': 'application/json',
        }

        data = { "accessToken":  self.settings.defaults()["circ.access_token"],
                 "refreshToken": self.settings.defaults()["circ.refresh_token"]}

        result = requests.post('https://node.goflash.com/login/refresh', headers=headers, data=json.dumps(data))

        data = result.json()
        self.settings.defaults()["circ.access_token"] = data["accessToken"]
        self.settings.defaults()["circ.refresh_token"] = data["refreshToken"]

        with open('settings.ini', 'w') as configfile:
            self.settings.write(configfile)



