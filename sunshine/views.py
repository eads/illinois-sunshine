from flask import Blueprint, render_template, abort, request, make_response, \
    session as flask_session
from flask.ext.cache import Cache
from sunshine.database import db_session
from sunshine.models import Candidate, Committee, Receipt, FiledDoc, Expenditure, D2Report
from sunshine.app_config import CACHE_CONFIG
import sqlalchemy as sa
import json
import time
from datetime import datetime, timedelta
from itertools import groupby
from operator import attrgetter
from dateutil.parser import parse

views = Blueprint('views', __name__)
cache = Cache(config=CACHE_CONFIG)
CACHE_TIMEOUT = 60*60*6

def make_cache_key(*args, **kwargs):
    path = request.path
    args = str(hash(frozenset(request.args.items())))
    # print 'cache_key:', (path+args)
    return (path + args).encode('utf-8')

@views.route('/')
@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
def index():
    engine = db_session.bind

    totals_sql = '''
      SELECT 
        SUM(total_amount) as total_amount, 
        SUM(donation_count) as donation_count, 
        AVG(average_donation) as average_donation 
      FROM receipts_by_week'''

    totals = list(engine.execute(sa.text(totals_sql)))

    recent_donations = db_session.query(Receipt)\
                                 .order_by(Receipt.received_date.desc())\
                                 .limit(10)
    
    committee_sql = ''' 
        SELECT * FROM (
          SELECT * 
          FROM committee_money
          WHERE committee_active = TRUE
          ORDER BY committee_name
        ) AS committees
        ORDER BY committees.total DESC NULLS LAST
        LIMIT 10
    '''
    
    top_ten = engine.execute(sa.text(committee_sql))

    return render_template('index.html', 
                           recent_donations=recent_donations,
                           top_ten=top_ten,
                           totals=totals)

@views.route('/donations/')
@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
def donations():

    start_date = datetime.now().date() - timedelta(days=7)
    end_date = datetime.now().date()

    if request.args.get('start_date'):
      start_date = parse(request.args.get('start_date'))
    
    if request.args.get('end_date'):
      end_date = parse(request.args.get('end_date'))

    prev_week_date = start_date - timedelta(days=7)
    next_week_date = end_date + timedelta(days=7)
    is_current = (end_date == datetime.now().date())
    
    recent_donations = db_session.query(Receipt)\
                                 .join(FiledDoc, Receipt.filed_doc_id == FiledDoc.id)\
                                 .filter(Receipt.received_date >= start_date)\
                                 .filter(Receipt.received_date <= end_date)\
                                 .order_by(FiledDoc.received_datetime.desc())
    
    donations_by_week = ''' 
        SELECT * FROM receipts_by_week
    '''

    engine = db_session.bind
    donations_by_week = [d.total_amount for d in engine.execute(donations_by_week)]

    totals_sql = '''
      SELECT 
        SUM(total_amount) as total_amount, 
        SUM(donation_count) as donation_count, 
        AVG(average_donation) as average_donation 
      FROM receipts_by_week'''

    totals = list(engine.execute(sa.text(totals_sql)))

    return render_template('donations.html', 
                           recent_donations=recent_donations,
                           donations_by_week=donations_by_week,
                           start_date=start_date,
                           end_date=end_date,
                           prev_week_date=prev_week_date,
                           next_week_date=next_week_date,
                           is_current=is_current,
                           totals=totals)

@views.route('/about/')
def about():
    return render_template('about.html')

@views.route('/search/')
def search():
    term = request.args.get('term')
    table_name = request.args.getlist('table_name')
    if table_name == []:
      table_name = ['candidates', 'committees', 'officers', 'receipts']

    return render_template('search.html', term=term, table_name=table_name)

@views.route('/candidates/<candidate_id>/')
@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
def candidate(candidate_id):
    try:
        candidate_id = int(candidate_id)
    except ValueError:
        return abort(404)
    candidate = db_session.query(Candidate).get(candidate_id)
    
    if not candidate:
        return abort(404)
    
    ie_committees = ''' 
        SELECT * FROM expenditures_by_candidate
        WHERE candidate_id = :candidate_id
    '''
    
    engine = db_session.bind

    ie_committees = list(engine.execute(sa.text(ie_committees), 
                                        candidate_id=candidate_id))

    return render_template('candidate-detail.html', 
                           candidate=candidate,
                           ie_committees=ie_committees)

@views.route('/top-earners/')
@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
def top_earners():
    engine = db_session.bind
    
    top_earners = ''' 
        SELECT 
          sub.*,
          c.name
        FROM (
          SELECT 
            SUM(amount) AS amount,
            committee_id
          FROM receipts
          WHERE received_date > :received_date
          GROUP BY committee_id
          ORDER BY amount DESC
        ) AS sub
        JOIN committees AS c
          ON sub.committee_id = c.id
    '''
    
    thirty_days_ago = datetime.now() - timedelta(days=30)

    top_earners = engine.execute(sa.text(top_earners),
                                 received_date=thirty_days_ago)

    return render_template('top-earners.html', top_earners=top_earners)


@views.route('/committees/')
@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
def committees():

    committee_type = "Candidate"
    type_arg = 'candidate'

    if request.args.get('type'):
      type_arg = request.args.get('type', 'candidate')
      if type_arg == "independent":
        committee_type = "Independent Expenditure"
      if type_arg == "action":
        committee_type = "Political Action"
      if type_arg == "party":
        committee_type = "Political Party"
      if type_arg == "ballot":
        committee_type = "Ballot Initiative"

    page = request.args.get('page', 1)
    offset = (int(page) * 50) - 50
    
    if committee_type == "Candidate":
      sql = '''
          SELECT * FROM (
            SELECT distinct ON (first_name, last_name)
            committee_money.*, candidates.*
            FROM committee_money 
            LEFT JOIN candidate_committees ON committee_money.committee_id = candidate_committees.committee_id
            LEFT JOIN candidates ON candidates.id = candidate_committees.candidate_id
            WHERE committee_type = :committee_type
            ORDER BY first_name, last_name, committee_name
          ) AS committees
          ORDER BY committees.total DESC NULLS LAST
          LIMIT 50
          OFFSET :offset
      '''
    else:
      sql = '''
        SELECT * FROM (
          SELECT *
          FROM committee_money 
          WHERE committee_type = :committee_type
          ORDER BY committee_name
        ) AS committees
        ORDER BY committees.total DESC NULLS LAST
        LIMIT 50
        OFFSET :offset
    '''
    
    engine = db_session.bind
    
    if not flask_session.get('%s_page_count' % type_arg):

        result_count = '''
            SELECT COUNT(*) AS count
            FROM committee_money
            WHERE committee_type = :committee_type
        '''
        
        result_count = engine.execute(sa.text(result_count), 
                                              committee_type=committee_type)\
                                              .first()\
                                              .count
        
        page_count = int(round(result_count, -2) / 50)
        
        flask_session['%s_page_count' % type_arg] = page_count
    
    else:
        
        page_count = flask_session['%s_page_count' % type_arg]

    committees = engine.execute(sa.text(sql), 
                                committee_type=committee_type,
                                offset=offset)
    
    return render_template('committees.html', 
                           committees=committees, 
                           committee_type=committee_type,
                           page_count=page_count)

@views.route('/committees/<committee_id>/')
@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
def committee(committee_id):
    try:
        committee_id = int(committee_id)
    except ValueError:
        return abort(404)
    committee = db_session.query(Committee).get(committee_id)
    
    if not committee:
        return abort(404)
    
    engine = db_session.bind
    
    latest_filing = ''' 
        SELECT * FROM most_recent_filings
        WHERE committee_id = :committee_id
        ORDER BY received_datetime DESC
        LIMIT 1
    '''

    latest_filing = engine.execute(sa.text(latest_filing), 
                                   committee_id=committee_id).first()
    
    params = {'committee_id': committee_id}

    if latest_filing.end_funds_available:

        recent_receipts = ''' 
            SELECT 
              COALESCE(SUM(receipts.amount), 0) AS amount
            FROM receipts
            JOIN filed_docs AS filed
              ON receipts.filed_doc_id = filed.id
            WHERE receipts.committee_id = :committee_id
              AND receipts.received_date > :end_date
        '''
        controlled_amount = latest_filing.end_funds_available + \
                            latest_filing.total_investments - \
                            latest_filing.total_debts
        
        params['end_date'] = latest_filing.reporting_period_end

    else:

        recent_receipts = ''' 
            SELECT 
              COALESCE(SUM(receipts.amount), 0) AS amount
            FROM receipts
            JOIN filed_docs AS filed
              ON receipts.filed_doc_id = filed.id
            WHERE receipts.committee_id = :committee_id
        '''
        
        controlled_amount = 0

    recent_total = engine.execute(sa.text(recent_receipts),**params).first().amount
    controlled_amount += recent_total
    
    candidate_ids = tuple(c.id for c in committee.candidates)
    
    related_committees = ''' 
        SELECT 
          name,
          id,
          type,
          active,
          money,
          reason
        FROM (
          (SELECT
            o.name,
            o.committee_id AS id,
            o.type,
            o.active,
            o.money,
            'Officers with the same name' AS reason
          FROM (
            SELECT
              cm.name,
              cm.active,
              o.committee_id,
              o.first_name,
              o.last_name,
              cm.type,
              m.total AS money
            FROM committees AS cm
            JOIN officers AS o
              ON cm.id = o.committee_id
            LEFT JOIN committee_money AS m
              ON cm.id = m.committee_id
          ) AS o
          JOIN (
            SELECT
              cm.name,
              o.committee_id,
              o.first_name,
              o.last_name,
              cm.type,
              m.total AS money
            FROM committees AS cm
            JOIN officers AS o
              ON cm.id = o.committee_id
            LEFT JOIN committee_money AS m
              ON cm.id = m.committee_id
          ) AS o2
            ON o.first_name = o2.first_name
            AND o.last_name = o2.last_name
          WHERE o.committee_id != o2.committee_id
            AND (o.committee_id = :committee_id OR o2.committee_id = :committee_id))
    '''
    
    params = {'committee_id': committee_id}
    

    if candidate_ids:
        
        unions = ''' 
              UNION
              (SELECT 
                cm.name, 
                cm.id,
                cm.type,
                cm.active,
                m.total AS money,
                'Supported candidates in common' AS reason 
              FROM committees AS cm
              LEFT JOIN candidate_committees AS cc
                ON cm.id = cc.committee_id
              LEFT JOIN candidates AS cd
                ON cc.candidate_id = cd.id
              LEFT JOIN committee_money AS m
                ON cm.id = m.committee_id
              WHERE cd.id IN :candidate_ids)
              UNION
              (SELECT
                 oc.name,
                 oc.id,
                 oc.type,
                 oc.active,
                 m.total AS money,
                 'Officer with same name as supported candidate' AS reason
               FROM candidates AS cd
               LEFT JOIN officers AS o
                 ON cd.first_name = o.first_name
                 AND cd.last_name = o.last_name
               LEFT JOIN committees AS oc
                 ON o.committee_id = oc.id
               LEFT JOIN committee_money AS m
                 ON oc.id = m.committee_id
               WHERE cd.id IN :candidate_ids
              )
        '''
        
        related_committees += unions

        params['candidate_ids'] = candidate_ids

    related_committees += ''' 
            ) AS s
            WHERE id != :committee_id
              AND active = TRUE
        '''
    
    related_committees = list(engine.execute(sa.text(related_committees),**params))

    current_officers = [officer for officer in committee.officers if officer.current]

    quarterlies = ''' 
        SELECT DISTINCT ON (f.doc_name, f.reporting_period_end)
          r.end_funds_available,
          r.total_investments,
          r.total_receipts,
          (r.debts_itemized * -1) as debts_itemized,
          (r.debts_non_itemized * -1) as debts_non_itemized,
          (r.total_expenditures * -1) as total_expenditures,
          f.reporting_period_end
        FROM d2_reports AS r
        JOIN filed_docs AS f
          ON r.filed_doc_id = f.id
        WHERE r.committee_id = :committee_id
        ORDER BY f.reporting_period_end ASC
    '''

    quarterlies = list(engine.execute(sa.text(quarterlies), 
                                 committee_id=committee_id))

    #print(quarterlies)

    ending_funds = [[r.end_funds_available, 
                     r.reporting_period_end.year,
                     r.reporting_period_end.month,
                     r.reporting_period_end.day] 
                     for r in quarterlies]

    investments = [[r.total_investments, 
                    r.reporting_period_end.year,
                    r.reporting_period_end.month,
                    r.reporting_period_end.day] 
                    for r in quarterlies]

    debts = [[(r.debts_itemized + r.debts_non_itemized), 
               r.reporting_period_end.year,
               r.reporting_period_end.month,
               r.reporting_period_end.day] 
               for r in quarterlies]

    return render_template('committee-detail.html', 
                           committee=committee, 
                           current_officers=current_officers,
                           related_committees=related_committees,
                           recent_receipts=recent_receipts,
                           recent_total=recent_total,
                           latest_filing=latest_filing,
                           controlled_amount=controlled_amount,
                           ending_funds=ending_funds,
                           investments=investments,
                           debts=debts)

@views.route('/contributions/<receipt_id>/')
@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
def contribution(receipt_id):
    try:
        receipt_id = int(receipt_id)
    except ValueError:
        return abort(404)
    receipt = db_session.query(Receipt).get(receipt_id)
    if not receipt:
        return abort(404)
    return render_template('contribution-detail.html', receipt=receipt)

@views.route('/expenditures/<expense_id>/')
@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
def expense(expense_id):
    try:
        expense_id = int(expense_id)
    except ValueError:
        return abort(404)
    expense = db_session.query(Expenditure).get(expense_id)
    if not expense:
        return abort(404)
    return render_template('expense-detail.html', expense=expense)
