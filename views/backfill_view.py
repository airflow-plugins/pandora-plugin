from airflow.utils.db import provide_session
from datetime import datetime, timedelta
from wtforms import DateTimeField

from flask import flash, Markup, redirect, request
from flask_admin import BaseView, expose
from flask_admin.form import DateTimePickerWidget
from flask_login import current_user, login_required
from flask_wtf import Form

from airflow.configuration import conf
from airflow.executors.celery_executor import execute_command, DEFAULT_QUEUE
from airflow.models import DagModel, DagRun, DagStat
from airflow.settings import Session
from airflow.utils.state import State


class CustomDateTimeForm(Form):
    start_date = DateTimeField("Start date", widget=DateTimePickerWidget())
    end_date = DateTimeField("End date", widget=DateTimePickerWidget())


# TODO: this form hides away a lot of options available in the CLI for clear and backfill. If special options are
# TODO: needed, then users will need to go through the server. This View will cover majority of cases though.
class BackfillView(BaseView):
    @expose('/', methods=['GET', 'POST'])
    @login_required
    def index(self):
        dags_folder = conf.get('core', 'dags_folder')

        session = Session()
        dags = session.query(DagModel).filter(DagModel.is_active.is_(True)).all()
        dag_list = sorted([x.dag_id for x in dags])
        session.close()

        command_list = ['backfill', 'clear']

        start_datetime = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(1)
        end_datetime = datetime.now().replace(hour=23, minute=59, second=0, microsecond=0) - timedelta(1)
        form = CustomDateTimeForm(data={'start_date': start_datetime, 'end_date': end_datetime})

        if request.method == 'POST':
            command = request.form["command"]
            dag_id = request.form["dag"]
            start_date = request.form["start_date"]
            end_date = request.form["end_date"]

            build_command = 'airflow {0} {1} --start_date "{2}" --end_date "{3}" --subdir {4}'.format(
                command, dag_id, start_date, end_date, dags_folder
            )

            if 'depends_on_past' in request.form.keys():
                build_command = ' '.join([build_command, '--ignore_first_depends_on_past'])
            if request.form["command"] == 'clear':
                build_command = ' '.join([build_command, '--no_confirm'])

            execute_command.apply_async(args=[build_command], queue=DEFAULT_QUEUE)

            if request.form["command"] == 'clear':
                self.clear_dag_runs(dag_id, start_date, end_date)

            markup_msg = '{0}, your <b>{1}</b> command is being processed for {2} between dates {3} - {4}.'.format(
                current_user.user.username.title(), command, dag_id, start_date, end_date
            )
            flash(Markup(markup_msg))
            url = self.create_airflow_url(dag_id, start_date, end_date)
            return redirect(url)

        return self.render("backfill.html", dags=dag_list, commands=command_list, form=form)

    @staticmethod
    @provide_session
    def create_airflow_url(dag_id, start_date, end_date):
        """
        Creates the airflow url to redirect to. Gets the host_server based on if it's a fabio url or host:port. Then
        queries the database for the execution date, which will be in the range of the start and end date. Can have
        multiple values so will only return the earliest result. If no results are found, it'll use the start date,
        which will just take you to the most recent dagrun for that dag in the UI.
        :param dag_id: Dag id name. String.
        :param start_date: Start date. String of form %Y-%m-%d %H:%M:%S.
        :param end_date: End date. String of form %Y-%m-%d %H:%M:%S.
        :return: Airflow URL to redirect to. String.
        """
        start_date = datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
        end_date = datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')
        host_server = conf.get('webserver', 'base_url')

        session = Session()
        try:
            dagrun_query_result = session.query(DagRun) \
                .filter(DagRun.dag_id == dag_id) \
                .filter(DagRun.execution_date >= start_date) \
                .filter(DagRun.execution_date < end_date) \
                .order_by(DagRun.execution_date.asc()) \
                .first()
            execution_date = dagrun_query_result.execution_date.isoformat()
        except:
            session.rollback()
            execution_date = start_date.isoformat()
        finally:
            session.close()

        url = '{0}/admin/airflow/graph?dag_id={1}&execution_date={2}'.format(host_server, dag_id, execution_date)
        return url

    @staticmethod
    @provide_session
    def clear_dag_runs(dag_id, start_date, end_date):
        """
        Clears all the DagRuns and corrects the DagStats for an interval passed in the clear command because the
        clear command only clears the TaskInstances.
        :param dag_id: Dag id name. String.
        :param start_date: Start date. String of form %Y-%m-%d %H:%M:%S.
        :param end_date: End date. String of form %Y-%m-%d %H:%M:%S.
        :return: None.
        """
        start_date = datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
        end_date = datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')
        session = Session()
        try:
            dagrun_query = session.query(DagRun) \
                .filter(DagRun.dag_id == dag_id) \
                .filter(DagRun.execution_date >= start_date) \
                .filter(DagRun.execution_date < end_date)
            dagrun_query_result = dagrun_query.all()
            # remove dagruns with this state for clear command
            for result in dagrun_query_result:
                session.delete(result)
            # fix DagStats
            for state in State.dag_states:
                removed_state_counts = dagrun_query.filter(DagRun.state == state).count()
                dagstat_query = session.query(DagStat) \
                    .filter(DagStat.dag_id == dag_id) \
                    .filter(DagStat.state == state)
                dagstat_query_result = dagstat_query.first()  # only one row every time
                dagstat_query_result.count = max(dagstat_query_result.count - removed_state_counts, 0)
            session.commit()
        except:
            session.rollback()
        finally:
            session.close()
