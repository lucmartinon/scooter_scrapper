from providers.provider import Provider
from scooter_position_log import ScooterPositionLog
import requests
import json
import logging
import uuid


class Frontend(Provider):
    frontend = True

    def __init__(self, name):
        self.name = name

    def get_scooters(self):
        url = f'https://{self.name}.frontend.fleetbird.eu/api/prod/v1.06/map/cars/'
        r = requests.get(url)
        spls = []
        if r.status_code == 200:
            scooters = r.json()
            for scooter in scooters:
                spls.append(ScooterPositionLog(
                    provider=self.name,
                    vehicle_id=scooter["carId"],
                    licence_plate=scooter["licencePlate"],
                    city=str.lower(scooter["city"]),
                    lat=scooter["lat"],
                    lng=scooter["lon"],
                    battery_level=scooter["fuelLevel"],
                    raw_data=scooter
                ))
        else:
            logging.warning(f"{r.status_code} received from {self.name}, body: {r.content}")
        return spls




