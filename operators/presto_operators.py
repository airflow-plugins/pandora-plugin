from airflow import AirflowException
from airflow.hooks.hive_hooks import HiveMetastoreHook
from airflow.operators.presto_check_operator import PrestoValueCheckOperator
from airflow.utils.decorators import apply_defaults


class PrestoOperationCheckOperator(PrestoValueCheckOperator):
    """
    Performs a simple logical validation check using sql code. This operator is meant to help do queries
    such as "is the result of this query greater than this value?" and things of that sort. The PrestoValueCheckOperator
    does not have that full flexibility, so this operator is an extension of it that build sql to fit the
    PrestoValueCheckOperator format.

    :param sql: The sql to be executed
    :type sql: string
    :param check_value: The value to check the comparison against
    :type: check_value: int
    :param operation: The logical operation to check the sql against the pass_value. Defaults to greater than.
        Expected values are `>`, `=`, `<`, `>=`, `<=`.
    :type operation: string
    :param conn_id: Reference to the Presto database
    :type conn_id: string
    """

    template_fields = ('sql', 'operation', 'presto_conn_id')

    @apply_defaults
    def __init__(self,
                 sql,
                 check_value,
                 operation='>',
                 conn_id='presto_default', *args, **kwargs):
        super(PrestoOperationCheckOperator, self).__init__(sql=sql, pass_value=check_value, conn_id=conn_id, *args, **kwargs)
        self.operation = operation
        self.presto_conn_id = conn_id

    def execute(self, context=None):
        original_sql, original_value = self.sql, self.pass_value
        base_sql = 'SELECT CASE WHEN ({sql}) {op} {val} THEN 1 ELSE 0 END'\
            .format(sql=self.sql, op=self.operation, val=self.pass_value)
        self.sql = base_sql
        self.pass_value = 1
        try:
            super(PrestoOperationCheckOperator, self).execute(context)
        except Exception as e:
            raise AirflowException("Query result for: '{}' was not '{}' passed value of '{}'\n{}"
                                   .format(original_sql, self.operation, original_value, e))


class PrestoDuplicateCheckOperator(PrestoOperationCheckOperator):
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
    :param presto_conn_id: Name of the presto connection to use.
    :type presto_conn_id: string
    :param metastore_conn_id: Name of the hive metastore connection to use.
    :type metastore_conn_id: string
    """
    template_fields = ('table', 'schema', 'partition', 'presto_conn_id', 'metastore_conn_id')
    ui_color = '#374665'
    ui_fgcolor = '#FFFFFF'

    @apply_defaults
    def __init__(self,
                 table,
                 schema='default',
                 partition="day='{{ ds }}'",
                 max_duplicates=0,
                 presto_conn_id='presto_default', metastore_conn_id='metastore_default', *args, **kwargs):
        super(PrestoDuplicateCheckOperator, self).__init__(
            sql='', check_value=max_duplicates, conn_id=presto_conn_id, operation='<=', *args, **kwargs)
        self.table = table
        self.schema = schema
        self.partition = partition
        self.presto_conn_id = presto_conn_id
        self.metastore_conn_id = metastore_conn_id

    def execute(self, context=None):
        metastore_hook = HiveMetastoreHook(metastore_conn_id=self.metastore_conn_id)
        table_metadata = metastore_hook.get_table(self.table, db=self.schema)
        is_partitioned = len(table_metadata.partitionKeys) > 0
        column_string = ', '.join([col.name for col in table_metadata.sd.cols])

        where_clause = 'WHERE {}'.format(self.partition) if is_partitioned else ''
        self.sql = "SELECT COUNT(*) FROM (SELECT {0}, COUNT(*) FROM {1}.{2} {3} GROUP BY {0} HAVING COUNT(*) > 1)" \
            .format(column_string, self.schema, self.table, where_clause)
        super(PrestoDuplicateCheckOperator, self).execute(context)


class PrestoCountCheckOperator(PrestoOperationCheckOperator):
    """
    This operator takes in a table and checks if it's count is greater than the `min_count`. If the table is partitioned
    it will also filter for only the partition statement passed. Otherwise it'll ignore it. By default that statement
    is "day='{{ ds }}'" as most of our tables are day partitioned. If you have multiple partitions you can pass in
    `a=b AND c=d`. The purpose of this operator is to have some validation on the data quality before marking
    a specific job/pipeline as SUCCESS.

    :param table: The name of the table to wait for.
    :type table: string
    :param schema: The schema the @table belongs to. Defaults to `default`.
    :type schema: string
    :param partition: The partition clause to wait for. This is passed as is to the metastore Thrift client
        ``get_partitions_by_filter`` method, and apparently supports SQL like notation as in ``day='2015-01-01'
        AND type='value'`` and comparison operators as in ``"day>=2015-01-01"``.
    :type partition: string
    :param min_count: The minimum number of records this table should have. Defaults to 0. Not inclusive.
    :type min_count: int
    :param presto_conn_id: Name of the presto connection to use.
    :type presto_conn_id: string
    :param metastore_conn_id: Name of the hive metastore connection to use.
    :type metastore_conn_id: string
    """
    template_fields = ('table', 'schema', 'partition', 'presto_conn_id', 'metastore_conn_id')
    ui_color = '#3b5998'
    ui_fgcolor = '#FFFFFF'

    @apply_defaults
    def __init__(self,
                 table,
                 schema='default',
                 partition="day='{{ ds }}'",
                 min_count=0,
                 presto_conn_id='presto_default', metastore_conn_id='metastore_default', *args, **kwargs):
        super(PrestoCountCheckOperator, self).__init__(
            sql='', check_value=min_count, conn_id=presto_conn_id, operation='>', *args, **kwargs)
        self.table = table
        self.schema = schema
        self.partition = partition
        self.presto_conn_id = presto_conn_id
        self.metastore_conn_id = metastore_conn_id

    def execute(self, context=None):
        metastore_hook = HiveMetastoreHook(metastore_conn_id=self.metastore_conn_id)
        table_metadata = metastore_hook.get_table(self.table, db=self.schema)
        is_partitioned = len(table_metadata.partitionKeys) > 0

        base_sql = "SELECT COUNT(*) FROM {}.{}".format(self.schema, self.table)
        if is_partitioned:
            base_sql += " WHERE {}".format(self.partition)

        self.sql = base_sql
        super(PrestoCountCheckOperator, self).execute(context)
