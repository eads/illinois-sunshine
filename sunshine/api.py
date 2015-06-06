import sqlalchemy as sa
from sunshine.database import db_session
from sunshine.models import Candidate, Candidacy, Committee, Officer, \
    D2Report, Receipt, Expenditure, Investment, candidate_committees
from flask import Blueprint, render_template, request, make_response
import json
from datetime import datetime, date
from collections import OrderedDict
from operator import attrgetter
from itertools import groupby

api = Blueprint('api', __name__)

dthandler = lambda obj: obj.isoformat() if isinstance(obj, date) else None

@api.route('/search/')
def search():
    term = request.args.get('term')
    tables = request.args.get('tables')
    resp = {
        'status': 'ok',
        'message': '',
        'objects': [],
    }
    status_code = 200
    if term:
        if tables:
            tables = tables.split(',')
        results = ''' 
            SELECT 
              s.id, 
              s.last_name,
              s.first_name,
              s.employer,
              s.description, 
              s.vendor_last_name, 
              s.vendor_first_name,
              s.amount,
              s.rank,
              s.received_date,
              c.id AS committee_id,
              c.name AS committee_name
            FROM (
              SELECT 
                id, 
                last_name, 
                first_name, 
                employer, 
                description,
                vendor_last_name, 
                vendor_first_name,
                amount,
                received_date,
                ts_rank_cd(search_index, query) AS rank,
                query,
                committee_id
              FROM receipts, 
                   to_tsquery(:term) AS query
              WHERE search_index @@ query
              ORDER BY rank DESC
            ) AS s
            JOIN committees AS c
              ON s.committee_id = c.id
            ORDER BY received_date DESC, rank DESC, amount DESC
            LIMIT 10
        '''
        engine = db_session.bind
        term = ' & '.join(term.split(' '))
        results = engine.execute(sa.text(results), term=term)
        fields = [c.name for c in results._cursor_description()]
        resp['objects'] = [OrderedDict(zip(fields, r)) for r in results]
    else:
        resp['status'] = 'error'
        resp['message'] = 'Search term required'
        status_code = 401

    response_str = json.dumps(resp, default=dthandler, sort_keys=False)
    response = make_response(response_str)
    response.headers['Content-Type'] = 'application/json'
    return response

@api.route('/committees/')
def committees():
    committee_table = Committee.__table__
    candidates_table = Candidate.__table__
    raw_query_params = request.args.copy()
    limit = request.args.get('limit', 500)
    offset = request.args.get('offset', 0)
    order_by = request.args.get('order_by', 'status_date')
    sort_order = request.args.get('sort_order', 'desc')
    if limit > 500:
        limit = 500
    valid_query, query_clauses, resp, status_code = make_query(committee_table, raw_query_params)
    if valid_query:
        committee_cols = [c.label('committee_%s' % c.name) for c in committee_table.columns]
        candidate_cols = [c.label('candidate_%s' % c.name) for c in candidates_table.columns]
        all_columns = committee_cols + candidate_cols
        base_query = db_session.query(*all_columns)\
                .join(candidate_committees)\
                .join(candidates_table)
        for clause in query_clauses:
            base_query = base_query.filter(clause)
        order_by_col = getattr(committee_table.c, order_by)
        base_query = base_query.order_by(getattr(order_by_col, sort_order)())
        base_query = base_query.limit(limit)
        objs = []
        committee_fields = committee_table.columns.keys() 
        candidate_fields = candidates_table.columns.keys()
        rows = sorted(list(base_query.all()), key=attrgetter('committee_id'))
        for committee, grouping in groupby(rows, attrgetter('committee_id')):
            rows = list(grouping)
            committee_values = rows[0][:len(committee_fields)]
            committee_info = OrderedDict(zip(committee_fields, committee_values))
            candidates = []
            for row in rows:
                candidate_values = row[len(committee_fields):]
                candidate_info = OrderedDict(zip(candidate_fields, candidate_values))
                candidates.append(candidate_info)
            committee_info['candidates'] = candidates
            objs.append(committee_info)
        resp['objects'] = objs
        resp['meta']['query'].update({
            'limit': limit,
            'offset': offset,
            'sort_order': sort_order,
            'order_by': order_by,
        })
    response = make_response(json.dumps(resp, default=dthandler, sort_keys=False))
    response.headers['Content-Type'] = 'application/json'
    return response

def make_query(table, raw_query_params):
    table_keys = table.columns.keys()
    args_keys = list(raw_query_params.keys())
    resp = {
        'meta': {
            'status': 'ok',
            'message': '',
            'query': {},
        },
        'objects': [],
    }
    status_code = 200
    query_clauses = []
    valid_query = True

    if 'offset' in args_keys:
        args_keys.remove('offset')
    if 'limit' in args_keys:
        args_keys.remove('limit')
    if 'order_by' in args_keys:
        args_keys.remove('order_by')
    for query_param in args_keys:
        try:
            field, operator = query_param.split('__')
        except ValueError:
            field = query_param
            operator = 'eq'
        query_value = raw_query_params.get(query_param)
        column = table.columns.get(field)
        if field not in table_keys:
            resp['meta']['message'] = '"%s" is not a valid fieldname' % field
            status_code = 400
            valid_query = False
        elif operator == 'in':
            query = column.in_(query_value.split(','))
            query_clauses.append(query)
            resp['meta']['query'][query_param] = query_value
        else:
            try:
                attr = next(filter(
                    lambda e: hasattr(column, e % operator),
                    ['%s', '%s_', '__%s__']
                )) % operator
            except IndexError:
                resp['meta']['message'] = '"%s" is not a valid query operator' % operator
                status_code = 400
                valid_query = False
                break
            if query_value == 'null': # pragma: no cover
                query_value = None
            query = getattr(column, attr)(query_value)
            query_clauses.append(query)
            resp['meta']['query'][query_param] = query_value

    return valid_query, query_clauses, resp, status_code
