import json
import os
from datetime import datetime

import requests
from feast import Feature, Entity, ValueType
from pyspark.sql.types import *


def _get_feast_type_from_spark(column, schema):
    feast_types_map = {
        "IntegerType": ValueType.INT64,
        "StringType": ValueType.STRING,
        "FloatType": ValueType.FLOAT,
        "DoubleType": ValueType.DOUBLE,
        "ByteType": ValueType.BYTES,
        "LongType": ValueType.INT64,
        "ShortType": ValueType.INT32,
        "TimestampType": ValueType.STRING,
        "BooleanType": ValueType.BOOL,
        "DateType": ValueType.STRING
    }
    column_type = str(schema[column].dataType)
    return feast_types_map.get(column_type, ValueType.STRING)


def _get_avro_type_from_spark(column, schema):
    avro_types_map = {
        "IntegerType": "long",
        "StringType": "string",
        "FloatType": "float",
        "DoubleType": "double",
        "ByteType": "bytes",
        "LongType": "long",
        "ShortType": "int",
        "TimestampType": "string",
        "BooleanType": "boolean",
        "DateType": "string"
    }
    column_type = str(schema[column].dataType)
    return avro_types_map.get(column_type, "string")


def _prepare_spark_dataframe(spark_df):
    pd_df = spark_df.toPandas()
    pd_df['datetime'] = int(datetime.now().timestamp()) * 1000
    return pd_df


def _get_result_schema(spark_df):
    result_schema = spark_df.schema.add("datetime", LongType(), False)
    return result_schema


def _check_dataframe_type(dataframe):
    df_type = str(type(dataframe))
    if df_type != "<class 'pyspark.sql.dataframe.DataFrame'>":
        raise TypeError(f"Wrong class type of dataframe: {df_type}!\n"
                        f"Supported only PySpark dataframe.")


def _generate_entity_rows(dataframe, table_entities):
    sp_df = dataframe.select(*table_entities)
    pd_df = sp_df.toPandas()
    entity_rows = []

    columns = list(pd_df.columns)
    for _, rows in pd_df.iterrows():
        entity_row = {col: rows[col] for col in columns}
        entity_rows.append(entity_row)

    return entity_rows


def _generate_batch_source_location(name, batch_location):
    base_location = "file:///home/tmp/feast"
    if batch_location:
        data_location = os.path.join(
            os.getenv("FEAST_SPARK_STAGING_LOCATION", batch_location), "{}".format(name))
    else:
        data_location = os.path.join(
            os.getenv("FEAST_SPARK_STAGING_LOCATION", base_location), "{}".format(name))
    return data_location


def _generate_feast_entities(entities, schema):
    entities_list = []
    for entity in entities:
        entity_type = _get_feast_type_from_spark(entity, schema)
        entities_list.append(Entity(name=entity,
                                    description=entity,
                                    value_type=entity_type))
    return entities_list


def _generate_feast_features(features, schema):
    features_list = []
    for feature in features:
        feature_type = _get_feast_type_from_spark(feature, schema)
        features_list.append(Feature(feature, feature_type))

    return features_list


def _generate_avro_schema(name, schema):
    avro_schema_fields = []
    for field in schema.fieldNames():
        field_avro_type = _get_avro_type_from_spark(field, schema)
        avro_schema_fields.append({"name": field, "type": field_avro_type})

    avro_schema_fields.append({"name": "datetime",
                               "type": {"type": "long", "logicalType": "timestamp-millis"}
                               })
    avro_schema_dict = {"type": "record",
                        "name": name,
                        "fields": avro_schema_fields}

    avro_schema_json = json.dumps(avro_schema_dict)
    return avro_schema_json


def _send_message_to_slack(text):
    data = {
        'text': text,
        'username': 'alert_bot',
        'icon_emoji': ':robot_face:'
    }
    requests.post('https://mattermost.test.ru/hooks/ja9f41qk3tynppgjqbpe5auz1c',
                  data=json.dumps(data), headers={'Content-Type': 'application/json'})
