from flask import Flask, render_template
from sunshine.views import views
from sunshine.api import api
import locale
import traceback
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
    
    @app.template_filter('format_money')
    def format_money(s):
        locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
        return locale.currency(s, grouping=True)
    
    @app.template_filter('donation_verb')
    def donation_verb(s):
        verbs = {
            '1A': 'donated',
            '2A': 'transferred',
            '3A': 'loaned',
            '4A': 'gave',
            '5A': 'donated inkind'
        }
        return verbs.get(s, 'donated')

    @app.template_filter('donation_name')
    def donation_name(s):
        verbs = {
            '1A': 'Donation',
            '2A': 'Transfer in',
            '3A': 'Loan recieved',
            '4A': 'Other',
            '5A': 'In kind donation'
        }
        return verbs.get(s, 'donated')
    
    @app.template_filter('expense_verb')
    def expense_verb(s):
        verbs = {
            '6B': 'transferred',
            '7B': 'loaned',
            '8B': 'spent',
            '9B': 'spent*',
        }
        return verbs.get(s, 'spent')

    @app.template_filter('expense_name')
    def expense_name(s):
        verbs = {
            '6B': 'Transfer out',
            '7B': 'Loan made',
            '8B': 'Expenditure',
            '9B': 'Independent Expenditure',
        }
        return verbs.get(s, 'spent')

    @app.template_filter('committee_description')
    def committee_description(s):
      if s == "Candidate":
        description = "Candidate committees accept campaign contributions and make expenditures under the candidate's authority in order to further their bid for election or re-election to public office. They are subject to state and federal contribution limits."
      elif s == "Independent Expenditure":
        description = "Independent expenditure committees, also known as Super PACs, may raise unlimited sums of money from corporations, unions, associations and individuals, then spend unlimited sums to overtly advocate for or against political candidates or issues. Unlike traditional PACs, independent expenditure committees are prohibited from donating money directly to political candidates."
      elif s == "Political Action":
        description = "A political action committee (PAC) is a type of organization that gathers campaign contributions from members and spends those funds to support or oppose candidates, ballot initiatives, or legislation. These committees are subject to state and federal contribution limits."
      elif s == "Political Party":
        description = "A political party committee is an organization, officially affiliated with a political party, which raises and spends money to support candidates of that party or oppose candidates of other parties. These committees are subject to some contributions limits, and funds are often transferred from Political Party Committees to Candidate Committees."
      elif s == "Ballot Initiative":
        description = "A ballot initiative is created by a petition signed by a minimum number of registered voters to bring about a public vote on a proposed statute or constitutional amendment. A group in support or opposition of this type of public policy is considered to be a ballot initiative committee. These committees are not subject to contributions limits."
      else:
        description = ""

      return description

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
