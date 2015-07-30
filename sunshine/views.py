from flask import Blueprint, render_template, abort, request, make_response, redirect, \
    session as flask_session
from sunshine.database import db_session
from sunshine.models import Candidate, Committee, Receipt, FiledDoc, Expenditure, D2Report
from sunshine.cache import cache, make_cache_key, CACHE_TIMEOUT
from sunshine.app_config import FLUSH_KEY
import sqlalchemy as sa
import json
import time
from datetime import datetime, timedelta
from itertools import groupby
from operator import attrgetter
from dateutil.parser import parse

views = Blueprint('views', __name__)

@views.route('/')
@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
def index():
    engine = db_session.bind

    # summary total text
    totals_sql = '''
      SELECT 
        SUM(total_amount) as total_amount, 
        SUM(donation_count) as donation_count, 
        AVG(average_donation) as average_donation 
      FROM receipts_by_month'''

    totals = list(engine.execute(sa.text(totals_sql)))

    # donations chart
    donations_by_month_sql = ''' 
      SELECT * FROM receipts_by_month WHERE month >= :start_date
    '''

    donations_by_month = [[d.total_amount,
                          d.month.year,
                          d.month.month,
                          d.month.day] 
                          for d in engine.execute(sa.text(donations_by_month_sql), 
                                                  start_date="1994-01-01")]

    # top earners in the last week
    top_earners = ''' 
        SELECT 
          sub.*,
          m.total,
          c.name,
          c.type
        FROM (
          SELECT 
            SUM(amount) AS amount,
            committee_id
          FROM condensed_receipts
          WHERE received_date > :received_date
          GROUP BY committee_id
          ORDER BY amount DESC
        ) AS sub
        JOIN committees AS c
          ON sub.committee_id = c.id
        JOIN committee_money as m
          ON c.id = m.committee_id
        ORDER BY sub.amount DESC
        LIMIT 10
    '''

    days_ago = datetime.now() - timedelta(days=30)
    top_earners = engine.execute(sa.text(top_earners),
                                 received_date=days_ago)
    
    # committees with the most money
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

    engine.dispose()

    return render_template('index.html', 
                           top_earners=top_earners,
                           top_ten=top_ten,
                           totals=totals,
                           donations_by_month=donations_by_month)

@views.route('/donations/')
@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
def donations():

    date = datetime.now().date() - timedelta(days=1)

    if request.args.get('date'):
      date = parse(request.args.get('date'))
    
    if request.args.get('date'):
      date = parse(request.args.get('date'))

    prev_day_date = date - timedelta(days=1)
    next_day_date = date + timedelta(days=1)
    is_current = (date == datetime.now().date())
    
    engine = db_session.bind

    days_donations_sql = '''
      SELECT 
        c.*, 
        cm.name as committee_name
      FROM condensed_receipts AS c 
      JOIN committees AS cm ON c.committee_id = cm.id
      WHERE c.received_date >= :start_date
        AND c.received_date < :end_date
      ORDER BY c.received_date DESC
    '''

    days_donations = list(engine.execute(sa.text(days_donations_sql), 
                                     start_date=date,
                                     end_date=(date + timedelta(days=1))))

    days_total_count = len(days_donations)
    days_total_donations = sum([d.amount for d in days_donations])

    engine.dispose()

    return render_template('donations.html', 
                           days_donations=days_donations,
                           days_total_count=days_total_count,
                           days_total_donations=days_total_donations,
                           date=date,
                           prev_day_date=prev_day_date,
                           next_day_date=next_day_date,
                           is_current=is_current)

@views.route('/about/')
def about():
    return render_template('about.html')

@views.route('/error/')
def error():
    return render_template('error.html')

@views.route('/search/')
def search():
    term = request.args.get('term')
    table_name = request.args.getlist('table_name')
    search_date__le = request.args.get('search_date__le')
    search_date__ge = request.args.get('search_date__ge')
    if table_name == []:
        table_name = ['candidates', 'committees', 'officers', 'receipts', 'expenditures']

    return render_template('search.html', 
                           term=term, 
                           table_name=table_name,
                           search_date__le=search_date__le,
                           search_date__ge=search_date__ge)

@views.route('/candidates/<candidate_id>/')
@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
def candidate(candidate_id):
    
    candidate_id = candidate_id.rsplit('-', 1)[-1]
    
    try:
        candidate_id = int(candidate_id)
    except ValueError:
        return abort(404)
    candidate = db_session.query(Candidate).get(candidate_id)
    
    if not candidate:
        return abort(404)

    supporting = [c for c in candidate.committees]
    opposing = []

    ie_committees = ''' 
        SELECT committee_id as id, committee_name as name, 
        committee_type as type, supporting, opposing, true as active 
        FROM expenditures_by_candidate
        WHERE candidate_id = :candidate_id
    '''
    
    engine = db_session.bind
    ie_committees = list(engine.execute(sa.text(ie_committees), 
                                        candidate_id=candidate_id))

    for ie in ie_committees:
      if ie.supporting:
        supporting.append(ie)
      if ie.opposing:
        opposing.append(ie)

    engine.dispose()

    return render_template('candidate-detail.html', 
                           candidate=candidate,
                           supporting=supporting,
                           opposing=opposing)

@views.route('/top-earners/')
@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
def top_earners():

    days_ago = 30
    if request.args.get('days_ago'):
      try: 
        days_ago = int(request.args.get('days_ago'))
      except:
        pass

    engine = db_session.bind
    
    top_earners = ''' 
        SELECT 
          sub.*,
          m.total,
          c.name,
          c.type
        FROM (
          SELECT 
            SUM(amount) AS amount,
            committee_id
          FROM condensed_receipts'''

    if days_ago > 0:
      top_earners += " WHERE received_date >= :received_date"

    top_earners += '''
          GROUP BY committee_id
          ORDER BY amount DESC
        ) AS sub
        JOIN committees AS c
          ON sub.committee_id = c.id
        JOIN committee_money as m
          ON c.id = m.committee_id
        ORDER BY sub.amount DESC
        LIMIT 100
    '''
    
    calc_days_ago = datetime.now() - timedelta(days=days_ago)

    top_earners = engine.execute(sa.text(top_earners),
                                 received_date=calc_days_ago)

    engine.dispose()

    return render_template('top-earners.html', 
                            top_earners=top_earners,
                            days_ago=days_ago,
                            calc_days_ago=calc_days_ago)


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
    
    engine.dispose()

    return render_template('committees.html', 
                           committees=committees, 
                           committee_type=committee_type,
                           page_count=page_count)

@views.route('/committees/<committee_id>/')
@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
def committee(committee_id):

    committee_id = committee_id.rsplit('-', 1)[-1]

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

    if latest_filing.end_funds_available \
        or latest_filing.end_funds_available == 0:

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

    supported_candidates = []
    opposed_candidates = []

    related_candidates_sql = ''' 
        SELECT first_name, last_name, office, candidate_id as id, 
        opposing, supporting, supporting_amount, opposing_amount
        FROM expenditures_by_candidate
        WHERE committee_id = :committee_id
        ORDER BY supporting_amount DESC, supporting ASC
    '''
    
    engine = db_session.bind
    related_candidates = list(engine.execute(sa.text(related_candidates_sql), 
                                        committee_id=committee.id))

    for c in related_candidates:
      if c.supporting:
        added = False
        for sc in supported_candidates:
          if sc['first_name'] == c.first_name and sc['last_name'] == c.last_name:
            added = True
            sc['office'] = sc['office'] + ", " + c.office
        if not added:
          supported_candidates.append(dict(c.items()))
      
      if c.opposing:
        added = False
        for sc in opposed_candidates:
          if sc['first_name'] == c.first_name and sc['last_name'] == c.last_name:
            added = True
            sc['office'] = sc['office'] + ", " + c.office
        if not added:
          opposed_candidates.append(dict(c.items()))

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

    donations = [[r.total_receipts, 
                  r.reporting_period_end.year,
                  r.reporting_period_end.month,
                  r.reporting_period_end.day] 
                  for r in quarterlies]

    expenditures = [[r.total_expenditures, 
                     r.reporting_period_end.year,
                     r.reporting_period_end.month,
                     r.reporting_period_end.day] 
                     for r in quarterlies]

    total_donations = sum([r.total_receipts for r in quarterlies])
    total_expenditures = sum([r.total_expenditures for r in quarterlies])

    engine.dispose()

    return render_template('committee-detail.html', 
                           committee=committee, 
                           supported_candidates=supported_candidates,
                           opposed_candidates=opposed_candidates,
                           current_officers=current_officers,
                           related_committees=related_committees,
                           recent_receipts=recent_receipts,
                           recent_total=recent_total,
                           latest_filing=latest_filing,
                           controlled_amount=controlled_amount,
                           ending_funds=ending_funds,
                           investments=investments,
                           debts=debts,
                           donations=donations,
                           expenditures=expenditures,
                           total_donations=total_donations,
                           total_expenditures=total_expenditures)

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

@views.route('/flush-cache/<secret>/')
def flush(secret):
    if secret == FLUSH_KEY:
        cache.clear()
    return 'Woop'

@views.route('/sunshine')
def sunshine():
    return redirect("/")
