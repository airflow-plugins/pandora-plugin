import fnmatch
import os
import subprocess

from airflow import configuration
from airflow.models import DagModel, DagRun, TaskInstance
from airflow.settings import Session, engine
from airflow.utils.state import State
from airflow.www.app import csrf

from flask import Blueprint, make_response
from prometheus_client import generate_latest, REGISTRY, Gauge

MetricsBlueprint = Blueprint(
    'metrics', __name__,
    url_prefix='/metrics'
)


def initialize_gauges():
    """
    Initialize gauges to track in prometheus. Sets the gauges in the global frame.
    """
    global dag_state, task_state, log_size_bytes, db_size_bytes, dagbag_size, postgres_connection
    dag_state = Gauge('airflow_dag_states', 'Number of DAG runs', ['state'])
    task_state = Gauge('airflow_task_states', 'Number of Task runs', ['state'])
    db_size_bytes = Gauge('airflow_db_size_bytes', 'Size of Postgres tables in bytes', ['table'])
    log_size_bytes = Gauge('airflow_log_size_bytes', 'Size of airflow logs in bytes', ['type'])
    dagbag_size = Gauge('airflow_dagbag_size', 'Number of dags')
    postgres_connection = Gauge('airflow_postgres_connections', 'Number of connections to this airflow database')


@MetricsBlueprint.route('/', methods=['GET'])
@csrf.exempt
def serve_metrics():
    """
    Calculate the metrics for the initialized gauges and send the stats to the http endpoint.
    """
    session = Session()

    dagbag_size.set(session.query(DagModel).filter(DagModel.is_active.is_(True)).count())

    for dag_state_type in State.dag_states:
        count = session.query(DagRun).filter(DagRun.state == dag_state_type).count()
        dag_state.labels(state=dag_state_type).set(count)

    # The SKIPPED state currently is not a part of Airflow.State.task_states, which is a bug.
    # A PR has been requested: https://github.com/apache/incubator-airflow/pull/2519 with the
    # following Jira issue AIRFLOW-1508 filed by Ace Haidrey, @ahaidrey.
    for task_state_type in State.task_states + (State.SKIPPED, ):
        count = session.query(TaskInstance).filter(TaskInstance.state == task_state_type).count()
        task_state.labels(state=task_state_type).set(count)
    session.close()

    # For querying table size in postgres
    result = engine.execute(
        "SELECT "
        "quote_ident(tablename), "
        "pg_total_relation_size(quote_ident(schemaname) || '.' || quote_ident(tablename))::BIGINT AS size "
        "FROM pg_tables WHERE schemaname = 'public';"
    )
    rows = result.fetchall()
    for row in rows:
        table, size = row
        db_size_bytes.labels(table=table).set(size)

    database = configuration.get('core', 'sql_alchemy_conn').rsplit('/', 1)[-1]
    result = engine.execute(
        "SELECT count(*) as count FROM pg_stat_activity WHERE datname=%s;", database
    )
    postgres_connection.set(result.fetchone()['count'])

    scheduler_size = 0
    worker_size = 0
    webserver_size = 0

    logs_path = configuration.get('core', 'BASE_LOG_FOLDER')

    logs = os.listdir(logs_path)
    for log in logs:
        log_path = os.path.join(logs_path, log)
        if os.path.isfile(log_path):
            if fnmatch.fnmatch(log, 'airflow-scheduler.log*'):
                scheduler_size += os.path.getsize(log_path)
            elif fnmatch.fnmatch(log, 'airflow-worker.log*'):
                worker_size += os.path.getsize(log_path)
            elif fnmatch.fnmatch(log, 'airflow-webserver.log*'):
                webserver_size += os.path.getsize(log_path)

    folder_size = int(subprocess.check_output(['du', '-sb', logs_path]).split('\t')[0])

    log_size_bytes.labels(type='scheduler').set(scheduler_size)
    log_size_bytes.labels(type='worker').set(worker_size)
    log_size_bytes.labels(type='webserver').set(webserver_size)
    log_size_bytes.labels(type='other').set(folder_size - scheduler_size - worker_size - webserver_size)

    stats = make_response(generate_latest(REGISTRY))
    stats.headers["content-type"] = "text/plain"

    return stats


if __name__ == "airflow.blueprints.metrics_blueprint":
    """
    Reason for this is so these metric gauges are initialized only once. Not every time the dag files are processed.
    """
    initialize_gauges()
