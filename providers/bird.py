from providers.provider import Provider, ScooterPositionLog
import requests
import json
import logging
import uuid


class Bird(Provider):
    provider = "bird"
    _base_url = "https://api.birdapp.com/"
    required_settings = ["bird.token"]

    uu_id = str(uuid.uuid1())
    version = "4.41.0"
    platform = "ios"
    ua = "Bird/4.41.0 (co.bird.Ride; build:37; iOS 12.3.1) Alamofire/4.41.0"

    def get_scooters(self, city):
        bird_token = self.settings["PROVIDERS"]["bird.token"]
        lat = city.lat
        lng = city.lng
        headers = {
            "Device-Id": self.uu_id,
            "App-Version": self.version,
            "Authorization": "Bird " + bird_token,
            "Location": json.dumps({
                "latitude": lat,
                "longitude": lng,
                "altitude": 500,
                "accuracy": 100,
                "speed": -1,
                "heading": -1
            })
        }
        radius = "10000"
        url = self._base_url + f"bird/nearby?latitude={lat}&longitude={lng}&radius={radius}"

        r = requests.get(url, headers=headers)
        spls = []
        if r.status_code == 200:
            scooters = r.json()["birds"]
            for scooter in scooters:
                spls.append(ScooterPositionLog(
                    provider=self.provider,
                    vehicle_id=scooter["id"],
                    city=city.name,
                    lat=scooter["location"]["latitude"],
                    lng=scooter["location"]["longitude"],
                    battery_level=scooter["battery_level"],
                    raw_data=scooter
                ))
        else:
            logging.warning(f"{r.status_code} received from {self.provider}, body: {r.content}")
        return spls




