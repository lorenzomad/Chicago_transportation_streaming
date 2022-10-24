"""Defines core consumer functionality"""
import logging


import confluent_kafka
from confluent_kafka import Consumer, OFFSET_BEGINNING
from confluent_kafka.avro import AvroConsumer, CachedSchemaRegistryClient
from confluent_kafka.avro.serializer import SerializerError
from tornado import gen


logger = logging.getLogger(__name__)


class KafkaConsumer:
    """Defines the base kafka consumer class"""

    def __init__(
        self,
        topic_name_pattern,
        message_handler,
        is_avro=True,
        offset_earliest=False,
        sleep_secs=1.0,
        consume_timeout=0.1,
    ):
        """Creates a consumer object for asynchronous use"""
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest

        
        BROKER_URL = "PLAINTEXT://localhost:9092"
        SCHEMA_REGISTRY_URL = "http://localhost:8081"
        
        self.broker_properties = {"bootstrap.servers": BROKER_URL, 'group.id':"0", "auto.offset.reset": "earliest" if offset_earliest else "latest"}
        

        if is_avro is True:
            
            schema_registry = CachedSchemaRegistryClient(SCHEMA_REGISTRY_URL)
            self.broker_properties["schema.registry.url"] = SCHEMA_REGISTRY_URL
            self.consumer = AvroConsumer(self.broker_properties)
        else:
            self.consumer = Consumer(self.broker_properties)
            

        #
        #
        # TODO: Configure the AvroConsumer and subscribe to the topics. Make sure to think about
        # how the `on_assign` callback should be invoked.
        #
        #
        self.consumer.subscribe( list(self.topic_name_pattern), on_assign= self.on_assign )

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""
                
        if self.offset_earliest == True:
            
            for partition in partitions:
                partition.offset = OFFSET_BEGINNING
                

        logger.info("partitions assigned for %s", self.topic_name_pattern)
        self.consumer.assign(partitions)

    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            await gen.sleep(self.sleep_secs)

    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""
        
        while True:
            message = c.poll(1.0)
            if message is None:
                return 0
            else:
                self.message_handler(message)
                logger.info(f"Consumer Message key :{message}")
                return 1
               


    def close(self):
        """Cleans up any open kafka consumers"""
        self.consumer.close()
