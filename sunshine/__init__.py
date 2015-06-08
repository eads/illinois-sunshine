from flask import Flask, render_template
from sunshine.views import views
from sunshine.api import api
import locale
from dateutil import parser

def create_app():
    app = Flask(__name__)
    config = '{0}.app_config'.format(__name__)
    app.config.from_object(config)
    app.register_blueprint(views)
    app.register_blueprint(api, url_prefix='/api')
    
    @app.errorhandler(404)
    def page_not_found(e):
        return render_template('404.html'), 404
    
    @app.template_filter('format_money')
    def format_money(s):
        locale.setlocale( locale.LC_ALL, '' )
        return locale.currency(s, grouping=True)
    
    return app
