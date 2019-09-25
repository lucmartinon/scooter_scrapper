from datetime import datetime
import json

class Provider():
    provider = None
    required_settings = []

    def __init__(self, settings):

        for setting in self.required_settings:
            if setting not in settings["PROVIDERS"]:
                raise Exception(f'{setting} has to be configured')

        self.settings = settings

    @classmethod
    def get_scooters(self, city):
        pass


class ScooterPositionLog():
    def __init__(self, vehicle_id: str, lat: float, lng: float, raw_data: str, provider: str, city: str,
                 secondary_id: str = None, licence_plate: str = None, timestamp: datetime = datetime.now(),
                 battery_level=None):
        self.city = city
        self.provider = provider
        self.id = vehicle_id
        self.secondary_id = secondary_id
        self.lat = lat
        self.lng = lng
        self.timestamp = timestamp
        self.battery_level = battery_level
        self.licence_plate = licence_plate
        self.raw_data = raw_data

    def to_dict(self):
        return {
            'city': self.city,
            'provider': self.provider,
            'id': self.id,
            'secondary_id': self.secondary_id,
            'lat': self.lat,
            'lng': self.lng,
            'timestamp': self.timestamp,
            'battery_level': self.battery_level,
            'licence_plate': self.licence_plate,
            'raw_data': json.dumps(self.raw_data)
        }
