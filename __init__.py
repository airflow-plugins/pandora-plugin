from airflow.plugins_manager import AirflowPlugin

from blueprints import PANDORA_BLUEPRINTS
from hooks import PANDORA_HOOKS
from menu_links import PANDORA_MENU_LINKS
from operators import PANDORA_OPERATORS
from sensors import PANDORA_SENSORS
from views import PANDORA_VIEWS


class PandoraAirflowPlugin(AirflowPlugin):
    name = 'pandora_airflow_plugin'
    operators = PANDORA_OPERATORS + PANDORA_SENSORS
    flask_blueprints = PANDORA_BLUEPRINTS
    hooks = PANDORA_HOOKS
    executors = []
    macros = []
    admin_views = PANDORA_VIEWS
    menu_links = PANDORA_MENU_LINKS
