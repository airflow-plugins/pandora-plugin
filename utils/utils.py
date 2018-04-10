import os
import shutil

from airflow.bin.cli import unpause
from airflow.configuration import conf
from airflow.models import DagModel, DagRun, DagStat, TaskInstance
from airflow.settings import Session
from airflow.utils.db import provide_session


@provide_session
def clear_dag(dag):
    """
    Delete all TaskInstances and DagRuns of the specified dag_id.
    :param dag: DAG object
    """
    session = Session()
    try:
        session.query(TaskInstance).filter(TaskInstance.dag_id == dag.dag_id).delete()
        session.query(DagRun).filter(DagRun.dag_id == dag.dag_id).delete()
        session.query(DagStat).filter(DagStat.dag_id == dag.dag_id).delete()
        session.commit()
        log_dir = conf.get('core', 'base_log_folder')
        full_dir = os.path.join(log_dir, dag.dag_id)
        shutil.rmtree(full_dir, ignore_errors=True)
    except:
        session.rollback()
    finally:
        session.close()


@provide_session
def unpause_dag(dag):
    """
    Wrapper around airflow.bin.cli.unpause. The issue is when we deploy the airflow dags they don't exist
    in the DagModel yet, so need to check if it exists first and then run the unpause.
    :param dag: DAG object
    """
    session = Session()
    try:
        dm = session.query(DagModel).filter(DagModel.dag_id == dag.dag_id).first()
        if dm:
            unpause(dag.default_args, dag)
    except:
        session.rollback()
    finally:
        session.close()


def td_format(td_object):
    if td_object.total_seconds == 0.0:
        return '0 seconds'

    seconds = int(td_object.total_seconds())
    if seconds == 0:
        return str(int(td_object.microseconds/1000)) + 'ms'
    periods = [
        ('year',        60*60*24*365),
        ('month',       60*60*24*30),
        ('day',         60*60*24),
        ('hour',        60*60),
        ('minute',      60),
        ('second',      1)
    ]

    strings = []
    for period_name, period_seconds in periods:
        if seconds > period_seconds:
            period_value, seconds = divmod(seconds,period_seconds)
            if period_value == 1:
                strings.append('%s %s' % (period_value, period_name))
            else:
                strings.append('%s %ss' % (period_value, period_name))

        return ', '.join(strings)
