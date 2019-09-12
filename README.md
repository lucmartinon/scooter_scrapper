# scooter_scrapper


## Intro
The objective of this project is to scrap data of positions of scooters like Lime / Bird / Cird, etc.
Contrary to a lot of existing projects, the objective here is to get an overview of *all* the scooters of a city and to *store* this data, to be able to analyse it afterwards.

### Current Status
Running the main will gather all the bikes of the city of Berlin, export all the data to a zipped csv, and insert this data in a postgres DB.

Lime needs a lot of query, so expect 20 / 30 minutes, most of it is waiting between each lime queries to not get a 429 answer

## Tech Details
The Project needs a `settings.ini` file in the root folder that contains the necessary IDs to make the request of each providers. This file should be as follow:

```
[DEFAULT]
circ.access_token =
circ.refresh_token =
lime.token =
bird.token =

[POSTGRES]
host = localhost
database = scooter_scrapper
user =
```

To see how to get the different token, the easiest is to refer to this list:
[https://github.com/ubahnverleih/WoBike](https://github.com/ubahnverleih/WoBike)
which explains in much details how to retrieve each of the token / necessary IDs. 



## Next Steps
* add more providers and more cities
* be smarter about Lime: more users to query faster, better query strategy...
* add a cron job that runs the process every X hours or X minutes

