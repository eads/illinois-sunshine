from flask import Flask, render_template, g
from flask_login import LoginManager
from sunshine.views import views
from sunshine.api import api
import locale
import traceback
from dateutil import parser
from sunshine.app_config import TIME_ZONE
from sunshine.cache import cache
from datetime import datetime
from sunshine import template_filters as tf
from sunshine.models import User
from .database import Base, db_session as session


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

    login_manager = LoginManager()
    login_manager.init_app(app)
    login_manager.login_view = '/admin/login/'

    if sentry:
        sentry.init_app(app)

    @login_manager.user_loader
    def load_user(user_id):
        return User.get(user_id)

    @app.errorhandler(404)
    def page_not_found(e):
        app.logger.info(traceback.format_exc())
        if sentry:
            sentry.captureException()
        return render_template('404.html'), 404

    @app.errorhandler(Exception)
    def error(e):
        app.logger.info(traceback.format_exc())
        if sentry:
            sentry.captureException()
        return render_template('error.html'), 500

    # register template filters from template_filters module
    app.jinja_env.filters['format_money'] = tf.format_money
    app.jinja_env.filters['format_money_short'] = tf.format_money_short
    app.jinja_env.filters['format_number'] = tf.format_number
    app.jinja_env.filters['format_large_number'] = tf.format_large_number
    app.jinja_env.filters['number_suffix'] = tf.number_suffix
    app.jinja_env.filters['donation_verb'] = tf.donation_verb
    app.jinja_env.filters['donation_name'] = tf.donation_name
    app.jinja_env.filters['expense_verb'] = tf.expense_verb
    app.jinja_env.filters['expense_name'] = tf.expense_name
    app.jinja_env.filters['expense_popover'] = tf.expense_popover
    app.jinja_env.filters['committee_description'] = tf.committee_description
    app.jinja_env.filters['slugify'] = tf.slugify
    app.jinja_env.filters['contested_races_description'] = tf.contested_races_description

    @app.context_processor
    def inject_date():
        return dict(now=datetime.now().replace(tzinfo=TIME_ZONE))

    @app.context_processor
    def data_quality_note():
        return dict(data_quality_note="We show data as it is reported by the <a href='http://elections.il.gov/' target='_blank'>Illinois State Board of Elections</a>. There are known issues. <a href='https://docs.google.com/spreadsheets/d/19yXMVn-hpO9mUrlnipLMnAEHm0kxVcqaegRPeYrtKSA/edit#gid=0' target='_blank'>We keep track of them here</a>.")

    @app.before_request
    def before_request():
        from sunshine.database import db_session

        g.engine = db_session.bind

    @app.teardown_request
    def teardown_request(exception):
        g.engine.dispose()

    return app
