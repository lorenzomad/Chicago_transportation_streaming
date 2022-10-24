"""Creates a turnstile data producer"""
import logging
from pathlib import Path

from confluent_kafka import avro

from models.producer import Producer
from models.turnstile_hardware import TurnstileHardware


logger = logging.getLogger(__name__)


class Turnstile(Producer):
    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_key.json")

    value_schema = avro.load(
        f"{Path(__file__).parents[0]}/schemas/turnstile_value.json"
    )

    def __init__(self, station):
        """Create the Turnstile"""
        station_name = (
            station.name.lower()
            .replace("/", "_and_")
            .replace(" ", "_")
            .replace("-", "_")
            .replace("'", "")
        )

        # I changed the topic to be only one, since the information on the station_id is already on the message, so i don't need separate topics
        # topic_name = f"Chicago.transport.turnstile.{station_name}"
        topic_name = f"Chicago.transport.turnstiles"
        super().__init__(
            topic_name, 
            key_schema=Turnstile.key_schema,
            value_schema=Turnstile.value_schema,
            num_partitions=2,
            num_replicas=1
        )
        self.station = station
        self.turnstile_hardware = TurnstileHardware(station)

    def run(self, timestamp, time_step):
        """Simulates riders entering through the turnstile."""
        num_entries = self.turnstile_hardware.get_entries(timestamp, time_step)
        
        
        for i in range(num_entries):
            self.producer.produce(
                topic=self.topic_name,
                key={"timestamp": self.time_millis()},
                value={
                    "station_id":self.station.station_id,
                    "station_name":self.station.name                
                },
                key_schema=self.key_schema,
                value_schema=self.value_schema
            )
