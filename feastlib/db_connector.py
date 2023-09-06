import yaml
import psycopg2
import traceback
import requests

from contextlib import closing
from datetime import datetime, timedelta


class FeastDatabaseConnector:
    def __init__(self, hostname, table):
        self.table = table
        self.schema = hostname.replace("-", "_").replace(".", "_")
        self.database_credentials = self._get_credentials()

        self.user = self.database_credentials.get("username")
        self.password = self.database_credentials.get("password")
        self.db_host = self.database_credentials.get("hostname")
        self.port = self.database_credentials.get("port")
        self.dbname = self.database_credentials.get("database")

    def _get_credentials(self):
        return yaml.load(open('./credentials.yml')).get("database")


    def get_all_tracked_tables(self):
        with closing(psycopg2.connect(dbname=self.dbname,
                                      user=self.user,
                                      password=self.password,
                                      host=self.db_host,
                                      port=self.port)) as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"""SELECT table_name FROM {self.schema}.{self.table}""")
                records = cursor.fetchall()
                tables_list = [tb[0] for tb in records]
                return tables_list

    def get_job_id_from_db(self, name):
        with closing(psycopg2.connect(dbname=self.dbname,
                                      user=self.user,
                                      password=self.password,
                                      host=self.db_host,
                                      port=self.port)) as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"""SELECT * FROM {self.schema}.{self.table} WHERE table_name = \'{name}\'""")
                record = cursor.fetchone()
                if record:
                    return record[1]

    def update_job_id_in_db(self, old_job_id, new_job_id):
        with closing(psycopg2.connect(dbname=self.dbname,
                                      user=self.user,
                                      password=self.password,
                                      host=self.db_host,
                                      port=self.port)) as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"""UPDATE {self.schema}.{self.table} SET job_id = \'{new_job_id}\' WHERE job_id = \'{old_job_id}\'""")
                conn.commit()

    def create_job_id_in_db(self, name, new_job_id):
        with closing(psycopg2.connect(dbname=self.dbname,
                                      user=self.user,
                                      password=self.password,
                                      host=self.db_host,
                                      port=self.port)) as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"""INSERT INTO {self.schema}.{self.table} VALUES (\'{name}\', \'{new_job_id}\')""")
                conn.commit()

    def log_create_new_feature_table(self, name, schema, age_to_store):
        try:
            with closing(psycopg2.connect(dbname=self.dbname,
                                          user=self.user,
                                          password=self.password,
                                          host=self.db_host,
                                          port=self.port)) as conn:
                delete_date = datetime.now() + timedelta(seconds=age_to_store)
                with conn.cursor() as cursor:
                    cursor.execute(f"""SELECT * FROM {self.schema}.{self.table}""")
                    records = cursor.fetchall()
                    tables_list = [tb[0] for tb in records]
                    if name in tables_list:
                        cursor.execute(f"""UPDATE {self.schema}.{self.table} 
                                         SET schema = \'{schema}\',
                                             delete_date = \'{delete_date}\'
                                         WHERE name = \'{name}\'""")
                        conn.commit()
                    else:
                        cursor.execute(f"""INSERT into {self.schema}.{self.table} 
                                         VALUES (\'{name}\',
                                                 \'{schema}\',
                                                 \'{delete_date}\')""")
                        conn.commit()
        except:
            wekbook_url = 'https://mattermost.test.ru/hooks/zyi55p7jyirz7kzu6hn7yqeyph'
            json = {'text': traceback.format_exc(),
                    'username': "logging_feast",
                    'icon_emoji': ':robot_face:'}
            response = requests.post(wekbook_url, headers={'Content-Type': 'application/json'}, json=json)




