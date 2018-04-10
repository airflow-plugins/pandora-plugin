import logging

from airflow.hooks.hdfs_hook import HDFSHook
from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from datetime import datetime


class HdfsLastModifiedDateSensor(BaseSensorOperator):
    """
    Waits until associated file has been modified in HDFS.
    This is to verify a non-partitioned table has been updated
    for the day before running jobs depending on the table.
    :param hdfs_path: Full HDFS path to file (e.g. "hdfs://analytics-namenode.savagebeast.com:9000/user/hadoop/musicops/albums")
    :type hdfs_path: string
    :param min_modification_date: Date of execution in the format of task execution time, i.e. YYYY-MM-DDThh:mm:ss.
    :type min_modification_date: string
    :param time_format: Time format to translate min_modification_date to, defaults to %Y-%m-%dT%H:%M:%S time format
    :type time_format: string
    :param hdfs_conn_id: Connection to the HDFS cluster.
    :type hdfs_conn_id: string
    """

    template_fields = ('min_modification_date', 'hdfs_path', 'time_format',)
    ui_color = '#C7E2A7'

    @apply_defaults
    def __init__(
            self,
            hdfs_path,
            min_modification_date=None,
            time_format='%Y-%m-%dT%H:%M:%S',
            hdfs_conn_id='hdfs_default',
            *args, **kwargs):
        super(HdfsLastModifiedDateSensor, self).__init__(*args, **kwargs)
        self.hdfs_path = hdfs_path
        self.min_modification_date = min_modification_date
        self.time_format = time_format
        self.hdfs_conn_id = hdfs_conn_id

    def poke(self, context):
        try:
            hdfs_hook = HDFSHook(hdfs_conn_id=self.hdfs_conn_id).get_conn()
            uri = hdfs_hook.df()['filesystem']

            # ls requires path to be relative e.g. "/Path/to/file"
            file_path = self.hdfs_path.replace(uri, '')

            logging.info('Poking: %s', file_path)
            # ls requires a list and returns a list. Only passing one path so list will always be one item.
            modification_time = list(hdfs_hook.ls([file_path], include_toplevel=True, include_children=False))[0][
                'modification_time']

            modification_date = datetime.fromtimestamp(modification_time / 1000)
            if self.min_modification_date:
                min_date = datetime.strptime(self.min_modification_date, self.time_format)
            else:
                min_date = context.get('task_instance').execution_date

            logging.info('%-25s: %s', 'Table last modified on', str(modification_date))
            logging.info('%-25s: %s', 'Task executed on', str(min_date))

            return modification_date >= min_date
        except Exception as e:
            logging.exception('Unexpected error while attempting to find modification date for %s', file_path)
            return False
