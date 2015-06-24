from flask import Blueprint, render_template, abort, request, make_response
from flask.ext.cache import Cache
from sunshine.database import db_session
from sunshine.models import Candidate, Committee, Receipt, FiledDoc, Expenditure, D2Report
from sunshine.app_config import CACHE_CONFIG
import sqlalchemy as sa
import json
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
    two_days_ago = datetime.now() - timedelta(days=3)
    
    recent_donations = db_session.query(Receipt)\
                                 .join(FiledDoc, Receipt.filed_doc_id == FiledDoc.id)\
                                 .filter(Receipt.received_date >= two_days_ago)\
                                 .order_by(FiledDoc.received_datetime.desc())\
                                 .limit(10)
    
    top_five = ''' 
        SELECT * FROM (
          SELECT DISTINCT ON(candidate_first_name, candidate_last_name)
            * 
          FROM candidate_money 
          WHERE total IS NOT NULL
            AND committee_type = 'Candidate'
          ORDER BY candidate_first_name, candidate_last_name
        ) AS rows
        ORDER BY rows.total DESC
        LIMIT 5
    '''
    
    engine = db_session.bind
    top_five = engine.execute(sa.text(top_five))

    return render_template('index.html', 
                           recent_donations=recent_donations,
                           top_five=top_five)

@views.route('/donations')
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

    return render_template('donations.html', 
                           recent_donations=recent_donations,
                           donations_by_week=donations_by_week,
                           start_date=start_date,
                           end_date=end_date,
                           prev_week_date=prev_week_date,
                           next_week_date=next_week_date,
                           is_current=is_current)

@views.route('/about/')
def about():
    return render_template('about.html')

@views.route('/candidates/')
@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
def candidates():
    # add pagination
    money = '''
        SELECT * FROM (
          SELECT DISTINCT ON(candidate_first_name, candidate_last_name)
            * 
          FROM candidate_money 
          WHERE total IS NOT NULL
            AND committee_type = 'Candidate'
          ORDER BY candidate_first_name, candidate_last_name
        ) AS rows
        ORDER BY rows.total DESC
        LIMIT 50
    '''
    engine = db_session.bind
    rows = engine.execute(sa.text(money))
    return render_template('candidates.html', rows=rows)

@views.route('/search/')
def search():
    term = request.args.get('term')
    table_name = request.args.getlist('table_name')
    if table_name == []:
      table_name = ['candidates', 'committees', 'officers', 'receipts']

    return render_template('search.html', term=term, table_name=table_name)

@views.route('/candidates/<candidate_id>/')
def candidate(candidate_id):
    try:
        candidate_id = int(candidate_id)
    except ValueError:
        return abort(404)
    candidate = db_session.query(Candidate).get(candidate_id)
    if not candidate:
        return abort(404)
    return render_template('candidate-detail.html', candidate=candidate)

@views.route('/committees/')
def committees():
    return render_template('committees.html')

@views.route('/committees/<committee_id>/')
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
        ORDER BY received_datetime
        LIMIT 1
    '''

    latest_filing = engine.execute(sa.text(latest_filing), 
                                   committee_id=committee_id).first()
    
    params = {'committee_id': committee_id}

    if latest_filing.end_funds_available:

        recent_receipts = ''' 
            SELECT 
              receipts.id,
              receipts.amount,
              receipts.first_name,
              receipts.last_name,
              filed.doc_name,
              filed.received_datetime
            FROM receipts
            JOIN filed_docs AS filed
              ON receipts.filed_doc_id = filed.id
            WHERE receipts.committee_id = :committee_id
              AND receipts.received_date > :end_date
            ORDER BY receipts.received_date DESC
        '''
        controlled_amount = latest_filing.end_funds_available
        params['end_date'] = latest_filing.reporting_period_end

    else:

        recent_receipts = ''' 
            SELECT 
              receipts.id,
              receipts.amount,
              receipts.first_name,
              receipts.last_name,
              filed.doc_name,
              filed.received_datetime
            FROM receipts
            JOIN filed_docs AS filed
              ON receipts.filed_doc_id = filed.id
            WHERE receipts.committee_id = :committee_id
            ORDER BY receipts.received_date DESC
        '''
        
        controlled_amount = 0

    recent_receipts = list(engine.execute(sa.text(recent_receipts),**params))
        
    recent_total = 0

    if recent_receipts:
        recent_total = sum([r.amount for r in recent_receipts])
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
          (r.total_expenditures * -1) as total_expenditures,
          r.total_receipts,
          f.reporting_period_end
        FROM d2_reports AS r
        JOIN filed_docs AS f
          ON r.filed_doc_id = f.id
        WHERE r.committee_id = :committee_id
        ORDER BY f.doc_name, f.reporting_period_end, f.received_datetime DESC
    '''

    quarterlies = list(engine.execute(sa.text(quarterlies), 
                                 committee_id=committee_id))
    
    oldest_report_date = quarterlies[0].reporting_period_end

    ending_funds = [r.end_funds_available for r in quarterlies]
    
    receipts_expenditures = [[r.total_receipts for r in quarterlies], \
                             [r.total_expenditures for r in quarterlies]]

    return render_template('committee-detail.html', 
                           committee=committee, 
                           current_officers=current_officers,
                           related_committees=related_committees,
                           recent_receipts=recent_receipts,
                           recent_total=recent_total,
                           latest_filing=latest_filing,
                           controlled_amount=controlled_amount,
                           ending_funds=ending_funds,
                           receipts_expenditures=receipts_expenditures,
                           oldest_report_date=oldest_report_date)

@views.route('/contributions/')
def contributions():
    contributions = db_session.query(Receipt)\
                        .order_by(Receipt.received_date.desc())\
                        .limit(100)
    return render_template('contributions.html', contributions=contributions)

@views.route('/contributions/<receipt_id>/')
def contribution(receipt_id):
    try:
        receipt_id = int(receipt_id)
    except ValueError:
        return abort(404)
    receipt = db_session.query(Receipt).get(receipt_id)
    if not receipt:
        return abort(404)
    return render_template('contribution-detail.html', receipt=receipt)

@views.route('/expenditures/')
def expenditures():
    expenditures = db_session.query(Expenditures)\
                        .order_by(Expenditures.expended_date.desc())\
                        .limit(100)
    return render_template('expenditures.html', expenditures=expenditures)

@views.route('/expenditures/<expense_id>/')
def expense(expense_id):
    try:
        expense_id = int(expense_id)
    except ValueError:
        return abort(404)
    expense = db_session.query(Expenditure).get(expense_id)
    if not expense:
        return abort(404)
    return render_template('expense-detail.html', expense=expense)

@views.route('/elections/')
def elections():
    return render_template('elections.html')

@views.route('/elections/<election_year>/<election_type>/')
def election(election_year, election_type):
    races = ''' 
        SELECT 
          c.id AS candidate_id, 
          c.last_name AS candidate_last_name, 
          c.first_name AS candidate_first_name,
          COALESCE(c.office, '') AS office,
          COALESCE(c.district, '') AS district,
          c.district_type,
          c.party,
          cc.race_type,
          cc.outcome
        FROM candidates AS c
        JOIN candidacies AS cc
          ON c.id = cc.candidate_id
        WHERE cc.election_type = :election_type
          AND cc.election_year = :election_year
    '''
    
    engine = db_session.bind
    races = engine.execute(sa.text(races), 
                           election_type=election_type, 
                           election_year=election_year)

    all_races = []
    races = sorted(races, key=attrgetter('office'))

    for office, office_grouping in groupby(races, key=attrgetter('office')):
        grouping = sorted(office_grouping, key=attrgetter('district'))
        d = {office: {}}
        for district, district_group in groupby(grouping, key=attrgetter('district')):
            d[office][district] = list(district_group)
        all_races.append(d)

    return render_template('election-detail.html', 
                           all_races=all_races,
                           election_year=election_year,
                           election_type=election_type)

