import io
import avro.schema
from avro.io import BinaryEncoder, DatumWriter
from confluent_kafka import Producer
from datetime import datetime

from feast import Client

from feastlib.db_connector import FeastDatabaseConnector


class FeastIngestionClassLight:
    """
    Lightweight copy of the main class FeastIngestionClass to run on cluster servers.

    Args:
        hostname (str): Hostname of a Feast instance
        table_name (str): Name of the feature table
        pandas_df (pandas.DataFrame): Pandas Dataframe from which to load data
    Attributes:
        hostname (str)
        table_name (str): Name of the feature table
        client (feast.Client): Feast Client for creating, managing, and retrieving features
        producer (feast.Producer): Feast Producer to Kafka
        feast_table (feast.feature_table.FeatureTable): Feast object of specified feature table
        avro_schema (json): Avro schema of Kafka topic
        topic (str): Kafka topic for specified feature table
        dataframe (pandas.DataFrame): Pandas Dataframe from which to load data
    """
    def __init__(self, hostname, table_name, pandas_df):
        self.hostname = hostname
        self.name = table_name
        self.client = Client(core_url=f"{self.hostname}:6565",
                             serving_url=f"{self.hostname}:6566",
                             job_service_url=f"{self.hostname}:6568")

        self.producer = Producer({"bootstrap.servers": f"{self.hostname}:9094", })
        self.feast_table = self.client.get_feature_table(self.name)
        self.avro_schema, self.topic = self._get_feature_table_info()

        self.dataframe = self._prepare_pandas_dataframe(pandas_df)

    def _get_feature_table_info(self):
        kafka_info = self.client.get_feature_table(self.name) \
            .to_dict() \
            .get('spec') \
            .get('streamSource') \
            .get('kafkaOptions')

        avro_schema = kafka_info.get('messageFormat') \
            .get('avroFormat') \
            .get('schemaJson')

        topic = kafka_info.get('topic')

        return avro_schema, topic

    def _send_avro_record_to_kafka(self, record):
        value_schema = avro.schema.Parse(self.avro_schema)
        writer = DatumWriter(value_schema)
        bytes_writer = io.BytesIO()
        encoder = BinaryEncoder(bytes_writer)
        writer.write(record, encoder)
        self.producer.produce(topic=self.topic, value=bytes_writer.getvalue())

    def _send_data_to_feast(self, df):
        num = 0
        for record in df.to_dict('records'):
            num = num + 1
            self._send_avro_record_to_kafka(record=record)
            if (num % 500 == 0):
                self.producer.flush()
        self.producer.flush()

    def _insert_to_redis(self, df):
        feast_table = self.client.get_feature_table(self.name)

        db_connector = FeastDatabaseConnector(hostname=self.hostname, table="running_jobs")
        tracked_tables = db_connector.get_all_tracked_tables()

        if self.name in tracked_tables:
            feast_table_job_id = db_connector.get_job_id_from_db(self.name)
            current_jobs = [i.get_id() for i in self.client.list_jobs(include_terminated=False)]
            if feast_table_job_id in current_jobs:
                self._send_data_to_feast(df)
            else:
                raise Exception('Job_ID was changed. Please update job_id from client mode.')
        else:
            raise Exception('Please create new job_id from client mode before running loading in cluster mode.')

    @staticmethod
    def _prepare_pandas_dataframe(pd_df):
        pd_df['datetime'] = int(datetime.now().timestamp()) * 1000
        return pd_df

    def load_data_to_feature_table(self):
        self._insert_to_redis(self.dataframe)
        return self.dataframe

