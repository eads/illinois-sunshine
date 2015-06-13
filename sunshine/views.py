from flask import Blueprint, render_template, abort, request, make_response
from sunshine.database import db_session
from sunshine.models import Candidate, Committee, Receipt
import sqlalchemy as sa
import json
from datetime import datetime, timedelta


views = Blueprint('views', __name__)

@views.route('/')
def index():
    two_days_ago = datetime.now() - timedelta(days=3)
    
    recent_donations = db_session.query(Receipt)\
                                 .filter(Receipt.received_date >= two_days_ago)\
                                 .order_by(Receipt.received_date.desc())

    return render_template('index.html', recent_donations=recent_donations)

@views.route('/about/')
def about():
    return render_template('about.html')

@views.route('/candidates/')
def candidates():
    money = '''
        SELECT * FROM candidate_money LIMIT 10
    '''
    engine = db_session.bind
    rows = engine.execute(sa.text(money))
    return render_template('candidates.html', rows=rows)

@views.route('/search/')
def search():
    return render_template('search.html')

@views.route('/candidate/<candidate_id>/')
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

@views.route('/committee/<committee_id>/')
def committee(committee_id):
    try:
        committee_id = int(committee_id)
    except ValueError:
        return abort(404)
    committee = db_session.query(Committee).get(committee_id)
    
    if not committee:
        return abort(404)
    
    engine = db_session.bind
    
    latest_quarterly = ''' 
        SELECT 
          d2.end_funds_available,
          fd.reporting_period_begin,
          fd.reporting_period_end
        FROM d2_reports AS d2
        JOIN filed_docs AS fd
          ON d2.filed_doc_id = fd.id
        WHERE d2.committee_id = :committee_id
          AND fd.doc_name = :type
        ORDER BY fd.received_datetime DESC
        LIMIT 1
    '''

    latest_quarterly = engine.execute(sa.text(latest_quarterly), 
                                      committee_id=committee_id,
                                      type='Quarterly').first()

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
    
    recent_receipts = list(engine.execute(sa.text(recent_receipts), 
                                          committee_id=committee_id,
                                          end_date=latest_quarterly.reporting_period_end))
    
    controlled_amount = latest_quarterly.end_funds_available
    recent_total = None

    if recent_receipts:
        recent_total = sum([r.amount for r in recent_receipts])
        controlled_amount += recent_total

    return render_template('committee-detail.html', 
                           committee=committee, 
                           recent_receipts=recent_receipts,
                           recent_total=recent_total,
                           latest_quarterly=latest_quarterly,
                           controlled_amount=controlled_amount)

@views.route('/contributions/')
def contributions():
    contributions = db_session.query(Receipt)\
                        .order_by(Receipt.received_date.desc())\
                        .limit(100)
    return render_template('contributions.html', contributions=contributions)

@views.route('/contribution/<receipt_id>/')
def contribution(receipt_id):
    try:
        receipt_id = int(receipt_id)
    except ValueError:
        return abort(404)
    receipt = db_session.query(Receipt).get(receipt_id)
    if not receipt:
        return abort(404)
    return render_template('contribution-detail.html', receipt=receipt)
