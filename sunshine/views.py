from flask import Blueprint, render_template, abort
from sunshine.database import db_session
from sunshine.models import Candidate, Committee
import sqlalchemy as sa

views = Blueprint('views', __name__)

@views.route('/')
def index():
    return render_template('index.html')

@views.route('/about/')
def about():
    return render_template('about.html')

@views.route('/candidates/')
def candidates():
    money = ''' 
        SELECT 
          filings.*,
          (filings.end_funds_available + additional.amount) AS total,
          additional.last_receipt_date
        FROM all_quarterly_filings AS filings
        JOIN (
          SELECT
            SUM(receipts.amount) AS amount,
            MAX(receipts.received_date) AS last_receipt_date,
            q.committee_id
          FROM all_quarterly_filings AS q
          JOIN receipts
            USING(committee_id)
          JOIN filed_docs as f
            ON receipts.filed_doc_id = f.id
          WHERE f.reporting_period_begin > q.reporting_period_end
          GROUP BY q.committee_id
        ) AS additional
          USING(committee_id)
        ORDER BY total DESC
    '''
    engine = db_session.bind
    rows = engine.execute(sa.text(money))
    return render_template('candidates.html', rows=rows)

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
    
    reports = ''' 
        SELECT DISTINCT ON(f.id) 
          f.doc_name, 
          money.amount,
          f.received_datetime 
        FROM filed_docs AS f 
        JOIN (
          SELECT 
            amount,
            filed_doc_id 
          FROM receipts 
          WHERE committee_id = :committee_id 
          UNION ALL 
          SELECT 
            end_funds_available AS amount, 
            filed_doc_id
          FROM d2_reports 
          WHERE committee_id = :committee_id
        ) AS money 
          ON f.id = money.filed_doc_id 
        WHERE f.committee_id = :committee_id 
        ORDER BY f.id DESC, f.reporting_period_end DESC
    '''
    
    engine = db_session.bind
    reports = engine.execute(sa.text(reports), committee_id=committee_id)
    
    money = ''' 
        SELECT 
          filings.*,
          (filings.end_funds_available + additional.amount) AS total,
          additional.last_receipt_date
        FROM all_quarterly_filings AS filings
        JOIN (
          SELECT
            SUM(receipts.amount) AS amount,
            MAX(receipts.received_date) AS last_receipt_date,
            q.committee_id
          FROM all_quarterly_filings AS q
          JOIN receipts
            USING(committee_id)
          JOIN filed_docs as f
            ON receipts.filed_doc_id = f.id
          WHERE f.reporting_period_begin > q.reporting_period_end
          GROUP BY q.committee_id
        ) AS additional
          USING(committee_id)
        WHERE filings.committee_id = :committee_id
        ORDER BY total DESC
    '''
    
    money = engine.execute(sa.text(money), committee_id=committee_id)
    
    return render_template('committee-detail.html', 
                           committee=committee, 
                           reports=reports,
                           money=money)
