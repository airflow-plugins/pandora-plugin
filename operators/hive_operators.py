from airflow import AirflowException
from airflow.hooks.hive_hooks import HiveMetastoreHook, HiveCliHook
from airflow.operators.hive_operator import HiveOperator
from airflow.utils.decorators import apply_defaults


class HiveDuplicateCheckOperator(HiveOperator):
    """
    This operator takes in a table and checks how many records in it are duplicated. The sql groups on every single
    column in the table and if it is partitioned it will also filter for only the partition statement passed. By default
    that statement is "day='{{ ds }}'" as most of our tables are day partitioned. If you have multiple partitions you
    can pass in `a=b AND c=d`. The purpose of this operator is to have some validation on the data quality before marking
    a specific job/pipeline as SUCCESS.

    :param table: The name of the table to wait for.
    :type table: string
    :param schema: The schema the @table belongs to. Defaults to `default`.
    :type schema: string
    :param partition: The partition clause to wait for. This is passed as is to the metastore Thrift client
        ``get_partitions_by_filter`` method, and apparently supports SQL like notation as in ``day='2015-01-01'
        AND type='value'`` and comparison operators as in ``"day>=2015-01-01"``.
    :type partition: string
    :param max_duplicates: The maximum number of duplicates this table can have. Defaults to 0.
    :type max_duplicates: int
    :param hive_cli_conn_id: Name of the hive cli connection to use.
    :type hive_cli_conn_id: string
    :param metastore_conn_id: Name of the hive metastore connection to use.
    :type metastore_conn_id: string
    """

    template_fields = ('table', 'schema', 'partition', 'hive_cli_conn_id', 'metastore_conn_id', 'mapred_queue')
    ui_color = '#388E3C'
    ui_fgcolor = '#FFFFFF'

    @apply_defaults
    def __init__(self,
                 table,
                 partition="day='{{ ds }}'",
                 max_duplicates=0,
                 metastore_conn_id='metastore_default', *args, **kwargs):

        super(HiveDuplicateCheckOperator, self).__init__(hql='', *args, **kwargs)
        self.table = table
        self.max_duplicates = max_duplicates
        self.partition = partition
        self.metastore_conn_id = metastore_conn_id

    def execute(self, context=None):
        try:
            metastore_hook = HiveMetastoreHook(metastore_conn_id=self.metastore_conn_id)
            table_metadata = metastore_hook.get_table(self.table, db=self.schema)
            is_partitioned = len(table_metadata.partitionKeys) > 0
            column_string = ', '.join([col.name for col in table_metadata.sd.cols])

            where_clause = 'WHERE {}'.format(self.partition) if is_partitioned else ''
            self.hql = "SELECT COUNT(col2sum) FROM (SELECT COUNT(1) AS col2sum FROM {}.{} {} GROUP BY {}) t2 " \
                       "WHERE t2.col2sum > 1".format(self.schema, self.table, where_clause, column_string)

            hook = HiveCliHook(hive_cli_conn_id=self.hive_cli_conn_id, mapred_queue=self.mapred_queue)
            hook.hive_cli_params = '-S'  # suppress hive junk output
            output = hook.run_cli(hql=self.hql, schema=self.schema)
            output_row = int(output.strip())

            if output_row > self.max_duplicates:
                raise AirflowException('There are {} duplicate records found whereas the max number of duplicates'
                                       ' can be {}'.format(output_row, self.max_duplicates))

        except Exception as e:
            raise AirflowException('An error occurred with the following duplicate check query:\n\t{}\n{}'
                                   .format(self.hql, e))
