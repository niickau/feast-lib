# feastlib

Python package with classes for working with Feast.

## Main functions

### 1. Module import and object creation.
```python
from feastlib.main_class import FeastIngestionClass

fl = FeastIngestionClass()
```

### 2. Creating a new table in Feast.
```python
sp_df = spark.sql("""SELECT id, surname FROM test_table""")
df_schema = sp_df.schema

fl.create_new_feature_table(name="new_table", schema=df_schema, entities=["id"], features=["surname"])
```

### 3. Loading data into an existing Feast table.
```python
sp_df = spark.sql("""SELECT id, surname FROM test_table""")

# Single node mode
fl.load_data_to_feature_table(name="feast_table", sp_df, mode="client")

# Cluster mode
fl.load_data_to_feature_table(name="feast_table", sp_df, mode="cluster")
```

### 4. Unloading data from Feast for all entities from the dataframe.
```python
sp_df = spark.sql("""SELECT id, surname FROM test_table""")

res = fl.get_data_from_feature_table(name="feast_table", sp_df)
```

### 5. Unloading data from Feast for specific entities only.
```python
entities = [{'id':'1'},
            {'id':'2'}]

res = fl.get_rows_from_feature_table(name="feast_table", entity_rows=entities)
```

### 6. Access to Feast Python API
```python
feast_client = fl.client
```

### 7. Requesting the amount of free RAM in Redis.
```python
free_memory = fl.get_remains_memory()
```
