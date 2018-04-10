from flask import Blueprint
from blueprints.metrics_blueprint import MetricsBlueprint

# need a main blueprint object
PluginBlueprint = Blueprint(
    'plugin', __name__,
    template_folder='../templates',
    static_folder='static',
    static_url_path='/static/'
)

PANDORA_BLUEPRINTS = [
    MetricsBlueprint,
    PluginBlueprint,
]
