import inspect
import logging
import os

from airflow.hooks.base_hook import BaseHook
from airflow.models import TaskInstance, Variable
from airflow.settings import Session
from airflow.utils.email import send_email
from airflow.utils.state import State
from datetime import datetime, timedelta
from slackclient import SlackClient


class GeneralNotificationHook(BaseHook):
    """
    Object that is primarily meant to be used as notification callables on success and failures.
    It sends an email with the option to send a slack message as well.

    DAGs and tasks have the option to set `on_failure_callback` and `on_success_callback` so a general use would be to
    set the `on_failure_callback` at the default_args level so it gets set for every task in the dag, and setting the
    `on_success_callback` only for the final task in your dag if it's to write back the dag status to a database or
    just creating a final PythonOperator to run this function as a callable.

    Examples and more in depth explanations can be found on this wiki:
        ... TODO
    """

    def __init__(self, args):
        """
        GeneralNotificationHook constructor.
        :param args: kv pairs of configs
        :type args: dict
        """
        super(GeneralNotificationHook, self).__init__(args)

        self.subject_fail = self.create_subject_message('Failure')
        self.subject_success = self.create_subject_message('Success')
        self.message_slack_fail = self.create_slack_message('Failure')
        self.message_slack_success = self.create_slack_message('Success')
        self.args = args
        self.sc = SlackClient(args['slack_api_token'])
        self.slack_api_params = {
            'channel': args['slack_channel'],
            'username': args['slack_bot'],
            'text': self.message_slack_fail,
            'icon_url': args['slack_avatar_icon_url'],
            'attachments': None,
        }

    @staticmethod
    def create_slack_message(state):
        """
        :param state: State of dagrun such as 'Success' or 'Failure' etc.
        :return: Standard body content for the slack message
        """
        jinja_confs = {
            'dagid': '{{ dag.dag_id }}',
            'date': "{{ macros.ds_format(ts, '%Y-%m-%dT%H:%M:%S', '%x %I:%M:%S %p') }}",
            'url_date': "{{ macros.ds_format(ts, '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d+%H%%3A%M%%3A%S') }}",
            'taskid': '{{ task.task_id }}',
            'owner': "{{ conf.get('operators', 'default_owner') }}",
            'url': "{{ conf.get('webserver', 'base_url') }}"
        }
        if state.lower() == 'failure':
            msg = 'DAG *{dagid}* for {date} has *failed* task {taskid} on the {owner} instance. Current task failures: ' \
                '<{url}/admin/taskinstance/?flt0_dag_id_equals={dagid}&flt1_state_equals=failed' \
                '&flt4_execution_date_equals={url_date}| Task Instances.>'
        if state.lower() == 'success':
            msg = 'DAG *{dagid}* has successfully processed {date} on the {owner} instance. <{url}/admin/airflow/gantt?' \
                  'dag_id={dagid}&execution_date={url_date}|Gantt chart.>'
        return msg.format(**jinja_confs)

    @staticmethod
    def create_subject_message(state):
        """
        :param state: State of dagrun such as 'Success' or 'Failure' etc.
        :return: Standard subject message for the email
        """
        jinja_confs = {
            'state': state,
            'owner': '{{ dag.owner }}',
            'dagid': '{{ dag.dag_id }}',
            'date': "{{ macros.ds_format(ts, '%Y-%m-%dT%H:%M:%S', '%x %I:%M:%S %p') }}",
            'instance': "{{ conf.get('operators', 'default_owner') }}"
        }
        return 'Airflow {state} of {owner} DAG {dagid} for date {date} on the {instance} instance.'.format(**jinja_confs)

    @staticmethod
    def td_format(td_object):
        if td_object.total_seconds == 0.0:
            return '0 seconds'

        seconds = int(td_object.total_seconds())
        if seconds == 0:
            return str(int(td_object.microseconds / 1000)) + 'ms'
        periods = [
            ('year', 60 * 60 * 24 * 365),
            ('month', 60 * 60 * 24 * 30),
            ('day', 60 * 60 * 24),
            ('hour', 60 * 60),
            ('minute', 60),
            ('second', 1)
        ]

        strings = []
        for period_name, period_seconds in periods:
            if seconds > period_seconds:
                period_value, seconds = divmod(seconds, period_seconds)
                if period_value == 1:
                    strings.append('%s %s' % (period_value, period_name))
                else:
                    strings.append('%s %ss' % (period_value, period_name))

            return ', '.join(strings)

    @staticmethod
    def message_completion():
        email_html = os.path.realpath(os.path.join('../templates', 'email.html'))
        with open(email_html, 'r') as f:
            message_completion = f.read()
        return message_completion

    def notify(self, context=None, success=False):

        ts = context['ts']
        dag = context['dag']
        did = dag.dag_id

        if success:
            context['dagrun_status'] = 'SUCCESS'
            context['dagrun_class'] = 'success'
        else:
            context['dagrun_status'] = 'FAILED'
            context['dagrun_class'] = 'failed'

        context['elapsed_time'] = 'unknown'
        task_id = 'unknown'

        session = Session()
        try:
            task_id = context['task'].task_id

            logging.info('Context task_id {}'.format(task_id))

            start_time = session.query(TaskInstance)\
                .filter(TaskInstance.dag_id == did)\
                .filter(TaskInstance.execution_date == ts)\
                .filter(TaskInstance.start_date != None)\
                .order_by(TaskInstance.start_date.asc())\
                .first().start_date

            context['start_time'] = start_time
            end_time = datetime.now()
            context['end_time'] = end_time
            context['elapsed_time'] = self.td_format(end_time - start_time) if (start_time and end_time) else 'N/A'

            task_instances = session.query(TaskInstance)\
                .filter(TaskInstance.dag_id == did)\
                .filter(TaskInstance.execution_date == ts)\
                .filter(TaskInstance.state != State.REMOVED)\
                .order_by(TaskInstance.end_date.asc())\
                .all()

            tis = []
            for ti in task_instances:
                if ti.task_id == task_id:
                    logging.info('Adjusting details for task_id: {}'.format(task_id))
                    # fix status/end/duration for the task which is causing a notification
                    ti.end_date = end_time
                    ti.state = 'success' if success else 'failed'
                    if not ti.duration:
                        # If the reporting task has no duration, make one based on the report time
                        ti.duration = self.td_format(ti.end_date - ti.start_date)

                if not ti.duration:
                    # If other tasks are still running, make duration N/A
                    ti.duration = 'N/A'
                else:
                    if not isinstance(ti.duration, str):
                        ti.duration = self.td_format(timedelta(seconds=ti.duration))

                tis.append(ti)

            context['task_instances'] = tis

            operators = sorted(
                    list(set([op.__class__ for op in dag.tasks])),
                    key=lambda x: x.__name__
            )
            context['operators'] = operators

            send_slack = self.args['send_slack_message'] if 'send_slack_message' in self.args else True
            if send_slack:
                slack_message = self.message_slack_success if success else self.message_slack_fail
                self.slack_api_params['text'] = context['task'].render_template(None, slack_message, context)
                self.sc.api_call('chat.postMessage', **self.slack_api_params)

            # don't spam email if multiple completions. spamming Slack is OK ;-)
            state_key = context['dag'].dag_id + '.state'
            dag_state = Variable.get(state_key, deserialize_json=True, default_var={})
            if not dag_state.has_key('history'):
                dag_state['history'] = {}
            history = dag_state['history']
            if not history.has_key(ts):
                history[ts] = {}
            date = history[ts]
            sent_email_key = 'sent_success_email' if success else 'sent_failure_email'
            if not date.has_key(sent_email_key):
                date[sent_email_key] = False

            send_multiple_failures = self.get_value_from_args('send_multiple_failures', False)
            send_success_email = self.get_value_from_args('send_success_emails', True)
            if (not success) and date[sent_email_key] and not send_multiple_failures:
                logging.info(
                    'Skipping failure email notification because one was already sent for {0} regarding date {1}'
                    .format(did, ts))
                # nothing to do here
            else:
                subject = self.subject_success if success else self.subject_fail
                title = context['task'].render_template(None, subject, context)
                body = context['task'].render_template(None, self.message_completion(), context)
                email_list = context['task'].email
                # conditions to send an email are if task failure or
                # if task succeeds and user wants to receive success emails
                if not success or (send_success_email and success):
                    if success:
                        email_list = self.get_value_from_args('success_email', email_list)
                    send_email(email_list, title, body)
                date[sent_email_key] = True
                Variable.set(state_key, dag_state, serialize_json=True)
        except Exception as e:
            logging.warn('Problem reading task state when notifying result of task: {0}'
                         '\nException reason: {1}'.format(task_id, e))
        finally:
            session.rollback()
            session.close()

    def get_value_from_args(self, param, default_value):
        val = default_value
        if param in self.args:
            if self.args[param]:
                val = self.args[param]
            elif type(self.args[param]) == bool:
                val = self.args[param]
        return val

    def notify_failed(self, context):
        self.notify(context=context, success=False)

    def notify_success(self, context):
        self.notify(context=context, success=True)


class GeneralNotifySuccess:
    def __init__(self, args):
        self.notifier = GeneralNotificationHook(args)

    def __call__(self, context):
        self.notifier.notify_success(context)


class GeneralNotifyFailed:
    def __init__(self, args):
        self.notifier = GeneralNotificationHook(args)

    def __call__(self, context):

        calling_func = inspect.stack()[1][3]
        if self.prevent_double_calling(context, calling_func):
            return

        if 'reason' in context and 'timeout' in context['reason']:
            self.notifier.message_slack_fail += \
                '\nThe failure is due to a _*dagrun_timeout*_. You can increase this value in your DAG object.'
        self.notifier.notify_failed(context)

    @staticmethod
    def prevent_double_calling(context, calling_func):
        """
        :param context: Context passed to callback function.
        :param calling_func: Function calling this callback. `handle_callback` is from DR and `handle_failure` is from TI.
        :return: True if need to prevent running both task and dag callbacks. False otherwise.
        """
        dag = context['dag']
        task = context['task']
        dag_callback = dag.on_failure_callback
        task_callback = task.on_failure_callback
        cls = GeneralNotifyFailed

        if not dag_callback or not task_callback:
            return False

        is_same_callback = isinstance(task_callback, cls) and isinstance(dag_callback, cls)
        if is_same_callback and calling_func == 'handle_failure':  # we will defer to running dag callback
            return True

        return False
