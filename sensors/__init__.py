from sensors.hdfs_sensors import HdfsLastModifiedDateSensor
from sensors.hive_sensors import HiveLastModifiedDateSensor, HivePartitionLastModifiedDateSensor
from sensors.postgres_sensors import PostgresTableSensor

PANDORA_SENSORS = [
    HdfsLastModifiedDateSensor,
    HiveLastModifiedDateSensor,
    HivePartitionLastModifiedDateSensor,
    PostgresTableSensor,
]
