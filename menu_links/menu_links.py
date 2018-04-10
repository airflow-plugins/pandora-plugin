from flask_admin.base import MenuLink

"""
By specifying a category that doesn't exist, a new tab will appear in the webserver, with the menu link objects
specified to it.
"""
pandora_plugin = \
    MenuLink(
        category='Plugins',
        name='Pandora-Plugin',
        url='https://github.com/airflow-plugins/pandora-plugin'
    )