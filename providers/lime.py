import requests
import logging
from providers.provider import Provider, ScooterPositionLog
from random import random
from time import sleep
from math import radians, cos, sin, asin, sqrt
from datetime import datetime



class Lime(Provider):
    provider = "lime"
    _base_url = "https://web-production.lime.bike/api/rider/v1/"
    required_settings = ["lime.token"]

    def get_scooters(self, city):
        ts = datetime.now()
        lat = city.sw_lat
        x = 0.0015
        y = 0.0004

        scooters = []
        scooters_ids_dict = {}
        i = 0
        line = 0
        requests_count = 0

        # holds the info about the  circles where we cannot find any unknown scooter for sure
        # list of tuples <lng,lat,radius>
        covered_circles = []

        while lat < city.ne_lat:
            lng = city.sw_lng
            line += 1

            while lng < city.ne_lng:
                covered = False
                for circle in covered_circles:
                    if haversine(lng, lat, circle[0], circle[1]) < circle[2]:
                        covered = True
                        break

                if not covered:
                    bikes = self.get_lime_bikes(lat, lng, x, retrying=False)
                    requests_count += 1
                    known_bikes, unknown_bikes = 0,0
                    for scooter in bikes:
                        bike_id = str(scooter["attributes"]["latitude"]) + str(scooter["attributes"]["longitude"])
                        if bike_id not in scooters_ids_dict:
                            scooters_ids_dict[bike_id] = 1
                            scooters.append(scooter)
                            unknown_bikes += 1
                        else:
                            known_bikes += 1

                    furthest_bike = bikes[len(bikes)-1]
                    radius = haversine(furthest_bike["attributes"]["longitude"], furthest_bike["attributes"]["latitude"], lng, lat)
                    if radius > 1:
                        radius = 1
                    covered_circles.append((lng,lat,radius))
                    logging.debug(f"request {requests_count}: {unknown_bikes} new bikes added, {known_bikes} known bikes seen again, skipping a circle of {radius} km around [{lat},{lng}]")

                    sleep(random() * 4)

                i += 1
                lng += x

            lat += y
        logging.info("Found " + str(len(scooters)) + f" Lime bikes in Berlin in {requests_count} requests, {requests_count - 1} requests avoided")

        spls = []

        for scooter in scooters:
            del scooter["attributes"]['rate_plan_short']
            spls.append(ScooterPositionLog(
                provider= self.provider,
                vehicle_id= scooter["id"],
                city= city.name,
                lat= scooter["attributes"]["latitude"],
                lng= scooter["attributes"]["longitude"],
                battery_level= scooter["attributes"]["battery_level"],
                licence_plate=scooter["attributes"]["plate_number"],
                raw_data=scooter["attributes"],
                timestamp=ts
            ))
        return spls

    def get_lime_bikes(self, lat, lng, x, retrying):
        params = {
            "user_latitude": lat,
            "user_longitude": lng,
            "ne_lat": lat + x,
            "ne_lng": lng + x,
            "sw_lat": lat - x,
            "sw_lng": lng - x,
            "zoom": 16
        }
        url = self._base_url + "views/map"
        headers = {
            "authorization": "Bearer " + self.settings["PROVIDERS"]["lime.token"]
        }
        r = requests.get(url, params=params, headers=headers)

        if r.status_code == 200:
            bikes = r.json()["data"]["attributes"]["bikes"]
            return bikes
        else:
            if not retrying:
                logging.info(f"error while getting lime bikes: {r.status_code}, waiting 30 seconds and retrying")
                # wait 30 sec and retry
                sleep(30)
                return self.get_lime_bikes(lat, lng, x, True)
            logging.error(f"problem while getting lime bikes: {r.status_code}, retrying did not solve the problem")
            return []





def haversine(lon1, lat1, lon2, lat2):
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    # Radius of earth in kilometers is 6371
    km = 6371* c
    return km
