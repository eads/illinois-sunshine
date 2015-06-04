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
        SELECT * FROM (
          SELECT DISTINCT ON (doc.doc_name, committee.id, committee.candidate_id)
            d2.end_funds_available,
            committee.id,
            doc.received_datetime,
            committee.candidate_id, 
            committee.candidate_last_name,
            committee.candidate_first_name
          FROM d2_reports AS d2
          JOIN (
            SELECT 
              cm.id,
              cand.id AS candidate_id,
              cand.first_name AS candidate_first_name,
              cand.last_name AS candidate_last_name
            FROM committees AS cm
            JOIN candidate_committees AS cc
              ON cm.id = cc.committee_id
            JOIN (
              SELECT DISTINCT ON (cd.district, cd.office)
                cd.id,
                cd.first_name, 
                cd.last_name
              FROM candidates AS cd
              JOIN candidacies AS cs
                ON cd.id = cs.candidate_id
              WHERE cs.outcome = :outcome
                AND cs.election_year >= :year
              ORDER BY cd.district, cd.office, cs.id DESC
            ) AS cand
              ON cc.candidate_id = cand.id
            WHERE cm.type = :committee_type
          ) AS committee
            ON d2.committee_id = committee.id
          JOIN filed_docs AS doc
            ON d2.filed_doc_id = doc.id
          WHERE doc.doc_name = :doc_name
          ORDER BY doc.doc_name, 
                   committee.id,
                   committee.candidate_id,
                   doc.received_datetime DESC
        ) AS rows 
        ORDER BY end_funds_available DESC
        LIMIT 10
    '''
    
    params = {
        'doc_name': 'Quarterly',
        'year': 2014,
        'outcome': 'won',
        'committee_type': 'Candidate'
    }

    engine = db_session.bind
    rows = engine.execute(sa.text(money), **params)
    print(dir(rows))
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
    return render_template('committee-detail.html', committee=committee)
