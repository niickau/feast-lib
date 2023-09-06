import asyncio
import io
import time
import math
import avro.schema
from avro.io import BinaryEncoder, DatumWriter
from confluent_kafka import Producer

from feast import Client, FeatureTable
from feast.data_source import FileSource, KafkaSource
from feast.data_format import ParquetFormat, AvroFormat
from pyspark.sql.functions import spark_partition_id, pandas_udf, PandasUDFType
from google.protobuf.duration_pb2 import Duration

from feastlib import helpers
from feastlib.db_connector import FeastDatabaseConnector
from feastlib.helpers import _send_message_to_slack
from feastlib.redis_info import throw_memory_warn_info, MonitorMemoryRemains


class FeastIngestionClass:
    """
    The main class for creating new Feast feature_tables, loading and getting data.

    Args:
        hostname (str): Hostname of a Feast instance
        grpc_timeout (int): Amount in ms of GRPC connection timeout
    Attributes:
        hostname (str)
        client (feast.Client): Feast Client for creating, managing, and retrieving features
        producer (feast.Producer): Feast Producer to Kafka
        name (str): Name of the new feature table (Attribute only for loading method)
        dataframe (pyspark.sql.dataframe.DataFrame): Pyspark Dataframe from which to load data
                                                                                (Attribute only for loading)
        topic (str): Kafka topic for specified feature table (Attribute only for loading method)
        avro_schema (json): Avro schema of Kafka topic (Attribute only for loading method)
    """
    def __init__(self, hostname, grpc_timeout=10):
        self.hostname = hostname
        self.client = Client(core_url=f"{self.hostname}:6565",
                             serving_url=f"{self.hostname}:6566",
                             job_service_url=f"{self.hostname}:6568",
                             options={"grpc_connection_timeout": str(grpc_timeout)})
        self.producer = Producer({"bootstrap.servers": f"{self.hostname}:9094"})
        self.monitor = MonitorMemoryRemains()

        self.name = None
        self.dataframe = None
        self.topic = None
        self.avro_schema = None

    def create_new_feature_table(self, name, schema, entities, features, days_to_store=3, seconds_to_store=None, batch_location=None):
        """
        Creating a new feature table in Feast from Pyspark Dataframe.

        Args:
            name (str): Name of the new feature table
            schema (pyspark.sql.types.StructType): Schema of Pyspark Dataframe
            entities (list): List of column names from Pyspark Dataframe
            features (list): List of column names from Pyspark Dataframe
            days_to_store (int): How long store data in table
            batch_location (str): Path to batch_location directory on the client server
        """
        self._cancel_job_id(name)
        throw_memory_warn_info()

        topic = name
        avro_schema = helpers._generate_avro_schema(name, schema)
        feast_entities = helpers._generate_feast_entities(entities, schema)
        feast_features = helpers._generate_feast_features(features, schema)
        batch_location = helpers._generate_batch_source_location(name, batch_location)
        age_to_store = seconds_to_store if seconds_to_store else 3600 * 24 * days_to_store

        db_connector = FeastDatabaseConnector(hostname=self.hostname, table="tables_metadata")
        db_connector.log_create_new_feature_table(name, schema, age_to_store)
        feast_table = FeatureTable(
            name=name,
            entities=entities,
            features=feast_features,
            stream_source=KafkaSource(
                event_timestamp_column="datetime",
                created_timestamp_column="datetime",
                bootstrap_servers=f"{self.hostname}:9094",
                topic=topic,
                message_format=AvroFormat(avro_schema)
            ),
            batch_source=FileSource(
                event_timestamp_column="datetime",
                created_timestamp_column="created",
                file_format=ParquetFormat(),
                file_url=batch_location,
                date_partition_column="date"
            ),
            max_age=Duration(seconds=age_to_store)
        )

        for entity in feast_entities:
            self.client.apply_entity(entity)
        self.client.apply_feature_table(feast_table)

    def load_data_to_feature_table(self, name, dataframe, mode="client"):
        """
        Load data from Pyspark Dataframe to specified feature table.

        Args:
            name (str): Name of the feature table in Feast
            dataframe (pyspark.sql.dataframe.DataFrame): Pyspark Dataframe from which to load data
            mode (str): Type of loading mode - client or cluster
        """
        self.name = name
        self.dataframe = dataframe
        self.avro_schema, self.topic = self._get_feature_table_info()

        throw_memory_warn_info()
        self._send_loading_warning_to_slack()

        if mode == "client":
            self._client_mode_run()
        elif mode == "cluster":
            self._cluster_mode_run()

    def get_data_from_feature_table(self, name, dataframe, addition_feature_list=None):
        """
        Generate entities from Pyspark Dataframe and getting corresponding features from feature table.

        Args:
            name (str): Name of the feature table in Feast
            dataframe (pyspark.sql.dataframe.DataFrame): Pyspark Dataframe from which to load data
            addition_feature_list (list): List of features from other tables in format {feature_table_name:feature}
        """
        helpers._check_dataframe_type(dataframe)
        online_features = self._get_features_from_feature_table(name)
        online_entities = self._get_entities_from_feature_table(name)

        if addition_feature_list:
            feature_refs = list(set(online_features.extend(addition_feature_list)))
        else:
            feature_refs = online_features
        entity_rows = helpers._generate_entity_rows(dataframe, online_entities)

        online_features = self.client.get_online_features(feature_refs=feature_refs,
                                                          entity_rows=entity_rows)
        return online_features.to_dict()

    def get_rows_from_feature_table(self, name, entity_rows, addition_feature_list=None):
        """
         Gets features from feature table only for specified entities.

         Args:
             name (str): Name of the feature table in Feast
             entity_rows (list): List of entities in format {entity:value}
             addition_feature_list (list): List of features from other tables in format {feature_table_name:feature}
         """
        online_features = self._get_features_from_feature_table(name)

        if addition_feature_list:
            feature_refs = list(set(online_features.extend(addition_feature_list)))
        else:
            feature_refs = online_features
        online_features = self.client.get_online_features(feature_refs=feature_refs,
                                                          entity_rows=entity_rows)
        return online_features.to_dict()

    def get_remains_memory(self):
        try:
            memory = self.monitor.calculate_memory_remains()
            return memory
        except (KeyError, ValueError) as e:
            print(f'API response structure has been changed: {e}')

    def _client_mode_run(self):
        """
         Starts the process of loading data to Kafka topic and inserting it to Redis.

         Args:
             name (str): Name of the feature table in Feast
         """

        num_rows = self.dataframe.count()
        if num_rows > 500000:
            num_chunks = math.ceil(num_rows / 250000)
            weights_list = [1.0] * num_chunks
            splits = self.dataframe.randomSplit(weights_list)

            for chunk in splits:
                self._insert_to_redis(helpers._prepare_spark_dataframe(chunk))
        else:
            self._insert_to_redis(helpers._prepare_spark_dataframe(self.dataframe))

    def _cluster_mode_run(self):
        """
         Distribute the process of data loading to several machines of the cluster.

         Args:
             name (str): Name of the feature table in Feast
             dataframe (pyspark.sql.dataframe.DataFrame): Pyspark Dataframe from which to load data
         """
        num_rows = self.dataframe.count()
        num_chunks = math.ceil(num_rows / 250000) if num_rows >= 500000 else 1
        result_schema = helpers._get_result_schema(self.dataframe)
        table_name = self.name
        hostname = self.hostname

        @pandas_udf(result_schema, PandasUDFType.GROUPED_MAP)
        def feast_distribution(chunk):
            import os
            from feastlib.light_class import FeastIngestionClassLight

            os.environ['ARROW_PRE_0_15_IPC_FORMAT'] = '1'
            os.environ['PYTHONIOENCODING'] = 'utf8'

            alfa_feast_light = FeastIngestionClassLight(hostname=hostname, table_name=table_name, pandas_df=chunk)
            loaded_chunk = alfa_feast_light.load_data_to_feature_table()

            return loaded_chunk

        spark_df = self.dataframe.repartition(num_chunks).groupby(spark_partition_id()).apply(feast_distribution)
        spark_df.count()

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
                job = self.client.start_stream_to_online_ingestion(feast_table)
                new_job_id = job.get_id()
                db_connector.update_job_id_in_db(feast_table_job_id, new_job_id)
                time.sleep(60)
                self._send_data_to_feast(df)
        else:
            job = self.client.start_stream_to_online_ingestion(feast_table)
            new_job_id = job.get_id()
            db_connector.create_job_id_in_db(self.name, new_job_id)
            time.sleep(60)
            self._send_data_to_feast(df)

    def _send_data_to_feast(self, df):
        num = 0
        for record in df.to_dict('records'):
            num += 1
            self._send_avro_record_to_kafka(record=record)
            if num % 500 == 0:
                self.producer.flush()
        self.producer.flush()

    def _send_avro_record_to_kafka(self, record):
        value_schema = avro.schema.Parse(self.avro_schema)
        writer = DatumWriter(value_schema)
        bytes_writer = io.BytesIO()
        encoder = BinaryEncoder(bytes_writer)
        writer.write(record, encoder)
        self.producer.produce(topic=self.topic, value=bytes_writer.getvalue())

    def _get_features_from_feature_table(self, name):
        features = [f"{name}:{feature.name}" for feature in self.client.get_feature_table(name).features]
        return features

    def _get_entities_from_feature_table(self, name):
        return self.client.get_feature_table(name).entities

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

    def _send_loading_warning_to_slack(self):
        ignore_feature_table_list = []
        if self.name not in ignore_feature_table_list:
            _send_message_to_slack(f'Data is loading into Feast for the table: {self.name}')

    def _cancel_job_id(self, name):
        db_connector = FeastDatabaseConnector(hostname=self.hostname, table="running_jobs")
        job_id = db_connector.get_job_id_from_db(name)
        if job_id:
            for i in self.client.list_jobs(include_terminated=False):
                if i.get_id() == job_id:
                    i.cancel()
                    break
