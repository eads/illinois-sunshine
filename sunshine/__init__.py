from flask import Flask, render_template
from sunshine.views import views, cache
from sunshine.api import api
import locale
from dateutil import parser
from sunshine.app_config import TIME_ZONE
from datetime import datetime

def create_app():
    app = Flask(__name__)
    config = '{0}.app_config'.format(__name__)
    app.config.from_object(config)
    app.register_blueprint(views)
    app.register_blueprint(api, url_prefix='/api')
    cache.init_app(app)
    
    @app.errorhandler(404)
    def page_not_found(e):
        return render_template('404.html'), 404
    
    @app.template_filter('format_money')
    def format_money(s):
        locale.setlocale( locale.LC_ALL, '' )
        return locale.currency(s, grouping=True)
    
    @app.context_processor
    def inject_date():
        return dict(now=datetime.now().replace(tzinfo=TIME_ZONE))
    
    return app
