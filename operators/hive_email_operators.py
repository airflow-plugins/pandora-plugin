import logging
import os
import re
import tempfile

from jinja2 import Environment, FileSystemLoader, BaseLoader

from airflow.configuration import conf
from airflow.hooks.hive_hooks import HiveCliHook
from airflow.operators.email_operator import EmailOperator
from airflow.utils.decorators import apply_defaults


class HiveEmailOperator(EmailOperator):
    """
    Runs a hql statement and generates a result based off of it. If you return too much data the email may fail;
    therefore if the number of rows returned is greater than 100, we add the results as an attachment.
    There is a default html content body that makes a table based off of the data returned with the column names,
    but it can be customized and templated with whatever you pass as the html_content.

    :param hql: The table to check it's existence
    :type hql: string
    :param to: list of emails to send the email to
    :type to: list or string (comma or semicolon delimited)
    :param subject: subject line for the email (templated)
    :type subject: string
    :param html_content: content of the email (templated), html markup is allowed,
        or path to the template file relative from the dag
    :type html_content: string
    :param schema: Name of hive schema (database) to run @hql against
    :type schema: string
    :param hive_cli_conn_id: Reference to the Hive database connection
    :type hive_cli_conn_id: string
    :param mapred_queue: queue used by the Hadoop CapacityScheduler
    :type  mapred_queue: string
    """
    template_fields = ('hql', 'schema', 'to', 'subject', 'html_content', 'hive_cli_conn_id', 'mapred_queue',)
    template_ext = ('.sql', '.hql',)
    ui_color = '#E6EE9C'

    @apply_defaults
    def __init__(self,
                 hql,
                 to,
                 subject,
                 html_content=None,
                 schema='default',
                 hive_cli_conn_id='hive_cli_default',
                 mapred_queue='adhoc_batch1', *args, **kwargs):
        self.hql = hql
        self.schema = schema
        self.hive_cli_conn_id = hive_cli_conn_id
        self.mapred_queue = mapred_queue
        self.cutoff = 100
        super(HiveEmailOperator, self).__init__(to=to, subject=subject, html_content=html_content, *args, **kwargs)

    def prepare_template(self):
        if self.hql:
            self.hql = re.sub('(\$\{(hiveconf:)?([ a-zA-Z0-9_]*)\})', '{{ \g<3> }}', self.hql)

    def execute(self, context):
        ti = context['ti']
        host, dagid, taskid, exectime = ti.hostname.split('.')[0], ti.dag_id, ti.task_id, ti.execution_date.isoformat()
        hook = HiveCliHook(
            hive_cli_conn_id=self.hive_cli_conn_id,
            mapred_queue=self.mapred_queue,
            mapred_job_name='Airflow HiveEmailOperator task for {}.{}.{}.{}'.format(host, dagid, taskid, exectime)
        )
        hook.hive_cli_params = '-S'  # suppress hive junk output
        output = hook.run_cli(hql=self.hql, schema=self.schema, hive_conf={'hive.cli.print.header': 'true'})

        output_rows = [line for line in output.split('\n') if line]
        col_names = output_rows[0].split('\t')
        output_rows = output_rows[1:]

        if len(output_rows) > self.cutoff:
            msg = 'The query returned > {} rows.. Adding tsv as an attachment.'.format(self.cutoff)
            logging.warn(msg)
            f = tempfile.NamedTemporaryFile(delete=False)
            f.write(output)
            f.close()
            self.files = [f.name]
            self.html_content = '{}<br>Dag id: {}<br>Task id: {}<br>Execution Time: {}'.format(msg, dagid,
                                                                                               taskid, exectime)
        else:
            context.update({
                'hql': self.hql,
                'rows': output_rows,
                'col_names': col_names
            })

            if not self.html_content:
                check_path = os.path.join(os.path.dirname(__file__), '..', 'templates', 'hive_email_default.html')
            else:
                dag_path = conf.get('core', 'dags_folder')
                check_path = os.path.join(dag_path, os.path.dirname(context['dag'].filepath),  self.html_content)

            if os.path.exists(check_path):
                path, filename = os.path.split(os.path.abspath(check_path))
                template = Environment(loader=FileSystemLoader(path)).get_template(filename)
                logging.info("Using templated file located at: {path}".format(path=check_path))
            else:
                template = Environment(loader=BaseLoader()).from_string(self.html_content)

            self.html_content = template.render(**context)

        super(HiveEmailOperator, self).execute(context)

        # delete the temp file after successfully attached to email
        if len(output_rows) > self.cutoff:
            os.unlink(f.name)
