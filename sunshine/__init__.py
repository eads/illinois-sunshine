from flask import Flask, render_template
from sunshine.views import views
from sunshine.api import api
import locale
from dateutil import parser
from sunshine.app_config import TIME_ZONE
from sunshine.cache import cache
from datetime import datetime

sentry = None
try:
    from raven.contrib.flask import Sentry
    from sunshine.app_config import SENTRY_DSN
    if SENTRY_DSN:
        sentry = Sentry(dsn=SENTRY_DSN) 
except ImportError:
    pass
except KeyError:
    pass

def create_app():
    app = Flask(__name__)
    config = '{0}.app_config'.format(__name__)
    app.config.from_object(config)
    app.register_blueprint(views)
    app.register_blueprint(api, url_prefix='/api')
    cache.init_app(app)
    
    if sentry:
        sentry.init_app(app)
    
    @app.errorhandler(404)
    def page_not_found(e):
        return render_template('404.html'), 404
    
    @app.template_filter('format_money')
    def format_money(s):
        locale.setlocale( locale.LC_ALL, '' )
        return locale.currency(s, grouping=True)

    @app.template_filter('format_number')
    def format_number(s):
        return '{:,}'.format(s)

    @app.template_filter('format_large_number')
    def format_large_number(n):
        import math
        millnames=['','Thousand','Million','Billion','Trillion']
        n = float(n)
        millidx=max(0,min(len(millnames)-1,
                          int(math.floor(math.log10(abs(n))/3))))
        return '%.1f %s'%(n/10**(3*millidx),millnames[millidx])
    
    @app.context_processor
    def inject_date():
        return dict(now=datetime.now().replace(tzinfo=TIME_ZONE))
    
    return app
