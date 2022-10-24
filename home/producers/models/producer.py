"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
        offset_earliest = False
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas
        self.offset_earliest = offset_earliest
        BROKER_URL = "PLAINTEXT://localhost:9092"
        SCHEMA_REGISTRY_URL = "http://localhost:8081"
        
        self.broker_properties = {"bootstrap.servers": BROKER_URL, "auto.offset.reset": "earliest" if offset_earliest else "latest"}
        
        #create a client 
        self.client = AdminClient(self.broker_properties)

        
        
        #
        #
        # TODO: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #
        #
        

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        schema_registry = CachedSchemaRegistryClient(SCHEMA_REGISTRY_URL)
        
        # TODO: Configure the AvroProducer
        self.producer = AvroProducer(
            self.broker_properties,
            schema_registry=schema_registry
        )
        
        

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        #
        #
        # TODO: Write code that creates the topic for this producer if it does not already exist on
        # the Kafka Broker.
        #
        #
        topics_list = self.client.list_topics()
        topic_exists = self.topic_name in topics_list.topics
        
        if topic_exists==False:
            futures = self.client.create_topics(
                [NewTopic(
                    topic= self.topic_name,
                    num_partitions=self.num_partitions,
                    replication_factor=self.num_replicas,
                    )
                ]
            )
            
            for topic, future in futures.items():
                try:
                    future.result()
                    print("topic created")
                except Exception as e:
                    print(f"failed to create topic {self.topic_name}: {e}")
                    raise
            
        else: 
            print("the topic already exists")
            
        
    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        #
        #
        # TODO: Write cleanup code for the Producer here
        #
        #
        logger.info("producer close incomplete - skipping")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
