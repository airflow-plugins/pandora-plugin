import logging

from airflow.hooks.hive_hooks import HiveMetastoreHook
from airflow.utils.decorators import apply_defaults

from sensors.hdfs_sensors import HdfsLastModifiedDateSensor


class HiveLastModifiedDateSensor(HdfsLastModifiedDateSensor):
    """
    Waits until associated file has been modified in Hive.
    This is to verify a non-partitioned table has been updated for the day before running jobs depending on the table.
    :param table: Hive table name (e.g. "drm"); Schema name is specified separately.
    :type table: string
    :param schema: Hive schema name; if not explicitly specified, this is set to "default".
    :type schema: string
    :param min_modification_date: Date of execution in the format of task execution time, i.e. YYYY-MM-DDThh:mm:ss.
    :type min_modification_date: string
    :param time_format: Time format to translate min_modification_date to, defaults to %Y-%m-%dT%H:%M:%S time format
    :type time_format: string
    :param metastore_conn_id: Connection to the Metastore cluster.
    :type metastore_conn_id: string
    :param hdfs_conn_id: Connection to the HDFS cluster.
    :type hdfs_conn_id: string
    """

    template_fields = ('min_modification_date', 'schema', 'table',)
    ui_color = '#AED581'

    @apply_defaults
    def __init__(
            self,
            table,
            schema='default',
            min_modification_date=None,
            time_format='%Y-%m-%dT%H:%M:%S',
            metastore_conn_id='metastore_default',
            hdfs_conn_id='hdfs_default',
            *args, **kwargs):
        super(HiveLastModifiedDateSensor, self).__init__(time_format=time_format, hdfs_path='', *args, **kwargs)
        self.schema = schema
        self.table = table
        self.min_modification_date = min_modification_date
        self.metastore_conn_id = metastore_conn_id
        self.hdfs_conn_id = hdfs_conn_id
        self.hdfs_path = None

    def poke(self, context):

        metastore_hook = HiveMetastoreHook(metastore_conn_id=self.metastore_conn_id)
        schema_table = '.'.join([self.schema, self.table])
        try:
            self.hdfs_path = metastore_hook.get_table(schema_table).sd.location
        except Exception as e:
            logging.exception('Unexpected error while attempting to retrieve metadata for %s', schema_table)
            return False

        return super(HiveLastModifiedDateSensor, self).poke(context)


class HivePartitionLastModifiedDateSensor(HdfsLastModifiedDateSensor):
    """
    Waits until associated file has been modified in Hive.
    This is to verify a partitioned table has been updated
    for the day before running jobs depending on the partition.
    :param table: Hive table name (e.g. "drm"); Schema name is specified separately.
    :type table: string
    :param partition: Hive table partition name (e.g. "day=2017-09-01").
    :type partition: string
    :param schema: Hive schema name; if not explicitly specified, this is set to "default".
    :type schema: string
    :param min_modification_date: Date of execution in the format of task execution time, i.e. YYYY-MM-DDThh:mm:ss.
    :type min_modification_date: string
    :param time_format: Time format to translate min_modification_date to, defaults to %Y-%m-%dT%H:%M:%S time format
    :type time_format: string
    :param metastore_conn_id: Connection to the Metastore cluster.
    :type metastore_conn_id: string
    :param hdfs_conn_id: Connection to the HDFS cluster.
    :type hdfs_conn_id: string
    """

    template_fields = ('min_modification_date', 'schema', 'table', 'partition',)
    ui_color = '#A5D6A7'

    @apply_defaults
    def __init__(
            self,
            table,
            partition=None,
            schema='default',
            min_modification_date=None,
            time_format='%Y-%m-%dT%H:%M:%S',
            metastore_conn_id='metastore_default',
            hdfs_conn_id='hdfs_default',
            *args, **kwargs):
        super(HivePartitionLastModifiedDateSensor, self).__init__(time_format=time_format, hdfs_path='', *args, **kwargs)
        self.schema = schema
        self.table = table
        self.partition = partition
        self.min_modification_date = min_modification_date
        self.metastore_conn_id = metastore_conn_id
        self.hdfs_conn_id = hdfs_conn_id
        self.hdfs_path = None

    def poke(self, context):

        metastore_hook = HiveMetastoreHook(metastore_conn_id=self.metastore_conn_id)
        schema_table = '.'.join([self.schema, self.table])
        try:
            if self.partition:
                hdfs_table_path = metastore_hook.get_table(schema_table).sd.location
                self.hdfs_path = hdfs_table_path + "/" + self.partition
            else:
                self.hdfs_path = metastore_hook.get_table(schema_table).sd.location
        except Exception as e:
            logging.exception('Unexpected error while attempting to retrieve metadata for %s', schema_table)
            return False

        return super(HivePartitionLastModifiedDateSensor, self).poke(context)
