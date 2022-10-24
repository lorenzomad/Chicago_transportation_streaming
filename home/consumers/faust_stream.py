"""Defines trends calculations for stations"""
import logging
from dataclasses import asdict, dataclass

import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
@dataclass
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool
    


# Faust will produce records to Kafka in this format
@dataclass
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
topic = app.topic("Chicago.transport.psql.stations", value_type = Station)

out_topic_name = "Chicago.transport.faust.stations"

out_topic = app.topic(out_topic_name, key_type= int, value_type = TransformedStation, partitions=1)

#declare the table
table = app.Table(
    "faust.transport",
    default=TransformedStation,
    partitions=1,
    changelog_topic=out_topic
)



@app.agent(topic)
async def station(stations_event):
    async for station in stations_event:
        transformed_station = TransformedStation(
            station_id = station.station_id,
            station_name = station.station_name,
            order = station.order,
            line = "red" if station.red else "blue" if station.blue else "green"
        )
        table[station.station_id] = transformed_station
        print(f"{station.station_id} : {table[station.station_id]}")

if __name__ == "__main__":
    app.main()
