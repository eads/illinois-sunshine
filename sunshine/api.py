import sqlalchemy as sa
from sunshine.database import db_session
from sunshine.models import Candidate, Candidacy, Committee, Officer, \
    D2Report, Receipt, Expenditure, Investment, candidate_committees
from sunshine.cache import cache, make_cache_key, CACHE_TIMEOUT
from flask import Blueprint, render_template, request, make_response, g
import json
from datetime import datetime, date
from collections import OrderedDict
from operator import attrgetter, itemgetter
from itertools import groupby
from string import punctuation
import re
import sqlalchemy as sa
from io import StringIO, BytesIO
import csv
import zipfile

api = Blueprint('api', __name__)

dthandler = lambda obj: obj.isoformat() if isinstance(obj, date) else None

operator_lookup = {
    'ge': '>=',
    'gt': '>',
    'le': '<=',
    'lt': '<'
}


def sanitizeSearchTerm(term):
    allowed_punctuation = set(['&', '|', '"', "'"])
    all_punctuation = set(punctuation)
    punct = "".join(all_punctuation - allowed_punctuation)
    term = re.sub(r"[{}]+".format(re.escape(punct)), " ", \
            term)

    term = term.replace('"', "'")
    term = re.sub(r"[']+", "'", term)

    quoted_strings_re = re.compile(r"('[^']*')")
    space_between_words_re = re.compile(r'([^ &|])[ ]+([^ &|])')
    spaces_surrounding_letter_re = re.compile(r'[ ]+([^ &|])[ ]+')
    multiple_operator_re = re.compile(r"[ &]+(&|\|)[ &]+")

    tokens = quoted_strings_re.split(term)
    processed_tokens = []
    for token in tokens:
        token = token.strip()

        if token in ['', "'"]:
            continue

        if token[0] != "'":
            token = spaces_surrounding_letter_re.sub(r' & \1 & ', token)

            token = space_between_words_re.sub(r'\1 & \2', token)

            token = re.sub(r'([^ &|]+)', r'\1:*', token)

        processed_tokens.append(token)

    term = " & ".join(processed_tokens)

    term = multiple_operator_re.sub(r" \1 ", term)

    return term.replace("'", "''")

def getSearchResults(term, 
                     table_name, 
                     q_params={}):
    
    if table_name in ['receipts', 'expenditures', 'investments']:
        
        if table_name in ['receipts', 'expenditures']:
            table_name = 'condensed_%s' % table_name

        result = ''' 
            SELECT *
            FROM (
              SELECT 
                c.name AS committee_name,
                r.*
              FROM {0} AS r
              JOIN committees AS c
                ON r.committee_id = c.id,
                   to_tsquery('english', :term) AS query
              WHERE query @@ r.search_name
            ) AS {0}
            WHERE 1=1
        '''.format(table_name)
    
    elif table_name == 'officers':
        result = ''' 
            SELECT 
              c.id AS committee_id,
              c.name AS committee_name,
              c.active AS committee_active,
              o.id,
              o.last_name, 
              o.first_name, 
              o.address1,
              o.address2,
              o.city,
              o.state,
              o.zipcode,
              o.title,
              o.phone,
              o.resign_date,
              o.redaction_requested,
              o.current
            FROM officers AS o
            JOIN officer_committees AS oc
              ON o.id = oc.officer_id
            JOIN committees AS c
              ON oc.committee_id = c.id
            WHERE to_tsquery('english', :term) @@ o.search_name
        '''.format(table_name)

    else:
        result = ''' 
            SELECT *
            FROM {0},
                 to_tsquery('english', :term) AS query
            WHERE query @@ search_name
        '''.format(table_name)
    
    if q_params:
        sa_table = sa.Table(table_name, 
                            sa.MetaData(), 
                            autoload=True, 
                            autoload_with=g.engine)

        valid_query, _, _, _  = make_query(sa_table, q_params)
        
        if valid_query:
            clauses = []
            for key in q_params.keys():
                if key != 'term':
                    try:
                        fieldname, operator = key.split('__')
                        operator = operator_lookup[operator]
                    except ValueError:
                        fieldname = key
                        operator = '='
                    clauses.append('%s %s :%s' % (fieldname, operator, key))
            
            result = '{0} AND {1}'.format(result, ' AND '.join(clauses))

            q_params['term'] = sanitizeSearchTerm(term)

        else:
            raise ValueError
    else:
        q_params = {'term': sanitizeSearchTerm(term)}

    q_params['term'] = sanitizeSearchTerm(term)

    results = g.engine.execute(sa.text(result), **q_params)
    
    return results

@api.route('/advanced-search/')
@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
def advanced_search():
    resp = {
        'status': 'ok',
        'message': '',
        'meta': {},
        'objects': {},
    }
    
    status_code = 200
    valid = True

    term = request.args.get('term')
    limit = request.args.get('limit', 50)
    offset = request.args.get('offset', 0)
    datatype = request.args.get('datatype')
    order_by_col = None
    reverse_sort = True

    if request.args.get('length'):
        limit = request.args['length']
    
    if request.args.get('start'):
        offset = request.args['start']
    
    if request.args.get('order[0][column]'):
        col_idx = request.args['order[0][column]']
        order_by_col = request.args['columns[' + str(col_idx) + '][data]']
        
        sort_order = request.args['order[0][dir]']
        reverse_sort = True
        if sort_order == 'asc':
            reverse_sort = False
    
    default_tables = ['committees', 'candidates', 'receipts', 'expenditures', 'officers']

    table_names = request.args.getlist('table_name')
   
    if not table_names:
        table_names = default_tables

    if not term:
        resp['status'] = 'error'
        resp['message'] = 'A search term is required'
        status_code = 400
        valid = False
    
    elif len(term) < 3:
        resp['status'] = 'error'
        resp['message'] = 'Search term must be at least 3 characters long'
        status_code = 400
        valid = False

    if valid:

        # Need to figure a way to do any column. This will 
        # just work for search_date for the time being
        
        q_params = {k:v for k,v in request.args.items() if k.startswith('search_date') and v}
        
        objects = {}
        
        for table_name in table_names:
            
            results = getSearchResults(term, 
                                       table_name, 
                                       q_params=q_params)
            
            objects[table_name] = [OrderedDict(zip(r.keys(), r.values())) for r in results]
        
        start_idx = int(offset)
        end_idx = int(offset) + int(limit)
        total_rows = 0
        
        if datatype == 'csv':
            
            zfoutp = BytesIO()
            with zipfile.ZipFile(zfoutp, 'w') as zf:

                for table_name, records in objects.items():
                    if records:
                        outp = StringIO()
                        writer = csv.writer(outp)
                        writer.writerow(list(records[0].keys()))
                        writer.writerows([list(r.values()) for r in records])
                        zf.writestr('%s.csv' % table_name, outp.getvalue())

            response = make_response(zfoutp.getvalue(), 200)
            
            filedate = datetime.now().strftime('%Y-%m-%d')
            response.headers['Content-Type'] = 'application/zip'
            fname = 'Illinois_Sunshine_Search_%s_%s.zip' % ('_'.join(term.split(' ')), filedate)
            response.headers['Content-Disposition'] = 'attachment; filename="%s"' % (fname)
            
            return response

        else:
            for table_name, records in objects.items():
                
                if table_name == 'receipts':
                    
                    receipts_col = order_by_col

                    if not order_by_col:
                        receipts_col = 'received_date'
                        reverse_sort = True
                    
                    records = sorted(records, 
                                     key=lambda r: r[receipts_col] if r[receipts_col] else "", 
                                     reverse=reverse_sort)
                    
                elif table_name == 'expenditures':
                    
                    exp_col = order_by_col

                    if not order_by_col:
                        exp_col = 'expended_date'
                        reverse_sort = True
                    
                    records = sorted(records, 
                                     key=lambda r: r[exp_col] if r[exp_col] else "", 
                                     reverse=reverse_sort)
         
                elif table_name == 'investments':
                    
                    inv_col = order_by_col

                    if not order_by_col:
                        inv_col = 'purchase_date'
                        reverse_sort = True
                    
                    records = sorted(records, 
                                     key=lambda r: r[inv_col] if r[inv_col] else "", 
                                     reverse=reverse_sort)
                
                elif table_name == 'committees':
                    
                    cmt_col = order_by_col

                    if not order_by_col:
                        cmt_col = 'name'
                        reverse_sort = False
                    
                    records = sorted(records, 
                                     key=lambda r: str(r[cmt_col]) if str(r[cmt_col]) else "", 
                                     reverse=reverse_sort)
         
                else:
                    
                    other_col = order_by_col

                    if not order_by_col:
                        other_col = 'last_name'
                        reverse_sort = False

                    records = sorted(records, 
                                     key=lambda r: str(r[other_col]) if str(r[other_col]) else "", 
                                     reverse=reverse_sort)
                
                total_rows += len(records)
                resp['objects'][table_name] = records[start_idx:end_idx]
         
            resp['meta'] = {
                'total_rows': total_rows,
                'limit': limit,
                'offset': offset,
                'term': term
            }
            resp['recordsTotal'] = total_rows
            resp['recordsFiltered'] = total_rows
        
            if request.args.get('draw'):
                resp['draw'] = int(request.args['draw'])

    response_str = json.dumps(resp, sort_keys=False, default=dthandler)
    response = make_response(response_str, status_code)
    response.headers['Content-Type'] = 'application/json'

    return response

@api.route('/top-money/')
@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
def top_money():
    resp = {
        'status': 'ok',
        'message': '',
        'meta': {},
        'objects': {},
    }
    status_code = 200
    committee_id = request.args.get('committee_id')
    table = request.args.get('type')
    if not committee_id or not table:
        resp['status'] = 'error'
        resp['message'] = 'Committee ID and transaction type are required'
        status_code = 400

    else:
        top_donors = ''' 
            SELECT 
              SUM(amount) AS total,
              first_name,
              last_name,
              MAX(search_date) AS total_date
            FROM condensed_{0}
            WHERE committee_id = :committee_id
            GROUP BY last_name, first_name
            ORDER BY total DESC NULLS LAST
            LIMIT 20
        '''.format(table)

        top_donors = g.engine.execute(sa.text(top_donors), 
                                    committee_id=committee_id)

        resp['objects'] = [OrderedDict(zip(r.keys(), r.values())) \
                               for r in top_donors]

        resp['meta']['total_rows'] = len(resp['objects'])

    response_str = json.dumps(resp, sort_keys=False, default=dthandler)
    response = make_response(response_str, status_code)
    response.headers['Content-Type'] = 'application/json'
    return response


@api.route('/committees/')
@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
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
                .join(candidate_committees, 
                    candidate_committees.c.committee_id == committee_table.c.id)\
                .join(candidates_table, 
                    candidate_committees.c.candidate_id == candidates_table.c.id)
        
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

@api.route('/receipts/')
@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
def receipts():
    
    raw_query_params = request.args.copy()
    limit = request.args.get('limit', 1000)
    offset = request.args.get('offset', 0)
    order_by = request.args.get('order_by', 'received_date')
    sort_order = request.args.get('sort_order', 'desc')
    datatype = request.args.get('datatype')
    
    receipts_table = sa.Table('condensed_receipts', sa.MetaData(), 
                              autoload=True, 
                              autoload_with=db_session.bind)

    valid_query, query_clauses, resp, status_code = make_query(receipts_table, raw_query_params)
    
    if not raw_query_params.get('committee_id'):
        resp = {
            'status' : 'error',
            'message' : 'A committee ID is required',
        }
        status_code = 400
        valid_query = False
    
    if valid_query:
        committees_table = Committee.__table__
        
        committee_cols = [c.label('committee_%s' % c.name) for c in committees_table.columns]
        receipt_cols = [c.label('receipt_%s' % c.name) for c in receipts_table.columns]
        all_columns = committee_cols + receipt_cols
        
        base_query = db_session.query(*all_columns)\
                .join(receipts_table, receipts_table.c.committee_id == committees_table.c.id)
        for clause in query_clauses:
            base_query = base_query.filter(clause)
        
        order_by_col = getattr(receipts_table.c, order_by)
        base_query = base_query.order_by(getattr(order_by_col, sort_order)())
        
        limit_query = base_query.limit(limit)
        limit_query = limit_query.offset(offset)

        objs = []
        committee_fields = committees_table.columns.keys() 
        receipt_fields = receipts_table.columns.keys()
        
        rows = sorted(list(limit_query.all()), key=attrgetter('committee_id'))
        committee_info = {'receipts': [], 'name': ''}
        for committee, grouping in groupby(rows, attrgetter('committee_id')):
            rows = list(grouping)
            committee_values = rows[0][:len(committee_fields)]
            committee_info = OrderedDict(zip(committee_fields, committee_values))
            receipts = []
            for row in rows:
                receipt_values = row[len(committee_fields):]
                receipt_info = OrderedDict(zip(receipt_fields, receipt_values))
                receipts.append(receipt_info)
            committee_info['receipts'] = receipts
            objs.append(committee_info)
        
        if datatype == 'csv':
            outp = StringIO()
            writer = csv.writer(outp)
            records = committee_info['receipts']
            
            if records:
                writer.writerow(list(records[0].keys()))
                writer.writerows([list(r.values()) for r in records])
            
            response = make_response(outp.getvalue(), 200)
            
            filedate = datetime.now().strftime('%Y-%m-%d')
            response.headers['Content-Type'] = 'text/csv'
            fname = 'Illinois_Sunshine_Committee_Receipts_%s_%s.csv' % ('_'.join(committee_info['name'].split(' ')), filedate)
            response.headers['Content-Disposition'] = 'attachment; filename="%s"' % (fname)
            
            return response

        total_rows = base_query.count()
        resp['objects'] = objs
        resp['meta']['query'].update({
            'limit': limit,
            'offset': offset,
            'sort_order': sort_order,
            'order_by': order_by,
        })
        resp['meta']['total_rows'] = total_rows

    response = make_response(json.dumps(resp, default=dthandler, sort_keys=False))
    response.headers['Content-Type'] = 'application/json'
    return response

@api.route('/expenditures/')
@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
def expenditures():

    raw_query_params = request.args.copy()
    limit = request.args.get('limit', 1000)
    offset = request.args.get('offset', 0)
    order_by = request.args.get('order_by', 'expended_date')
    sort_order = request.args.get('sort_order', 'desc')
    datatype = request.args.get('datatype')

    expenditures_table = sa.Table('condensed_expenditures', sa.MetaData(), 
                                  autoload=True, autoload_with=db_session.bind)
    
    valid_query, query_clauses, resp, status_code = make_query(expenditures_table, raw_query_params)
    
    if not raw_query_params.get('committee_id'):
        resp = {
            'status' : 'error',
            'message' : 'A committee ID is required',
        }
        status_code = 400
        valid_query = False
    
    if valid_query:
        committees_table = Committee.__table__
        
        committee_cols = [c.label('committee_%s' % c.name) for c in committees_table.columns]
        expenditure_cols = [c.label('expenditure_%s' % c.name) for c in expenditures_table.columns]
        all_columns = committee_cols + expenditure_cols
        
        base_query = db_session.query(*all_columns)\
                         .join(expenditures_table, 
                               expenditures_table.c.committee_id == committees_table.c.id)

        for clause in query_clauses:
            base_query = base_query.filter(clause)
        
        order_by_col = getattr(expenditures_table.c, order_by)
        base_query = base_query.order_by(getattr(order_by_col, sort_order)())
        limit_query = base_query.limit(int(limit))
        limit_query = limit_query.offset(int(offset))

        objs = []
        committee_fields = committees_table.columns.keys() 
        expenditure_fields = expenditures_table.columns.keys()
        rows = sorted(list(limit_query.all()), key=attrgetter('committee_id'))
        committee_info = {'expenditures': [], 'name': ''}
        for committee, grouping in groupby(rows, attrgetter('committee_id')):
            rows = list(grouping)
            committee_values = rows[0][:len(committee_fields)]
            committee_info = OrderedDict(zip(committee_fields, committee_values))
            expenditures = []
            for row in rows:
                expenditure_values = row[len(committee_fields):]
                expenditure_info = OrderedDict(zip(expenditure_fields, expenditure_values))
                expenditures.append(expenditure_info)
            committee_info['expenditures'] = expenditures
            objs.append(committee_info)
        
        if datatype == 'csv':
            outp = StringIO()
            writer = csv.writer(outp)
            records = committee_info['expenditures']
            
            if records:
                writer.writerow(list(records[0].keys()))
                writer.writerows([list(r.values()) for r in records])
            
            response = make_response(outp.getvalue(), 200)
            
            filedate = datetime.now().strftime('%Y-%m-%d')
            response.headers['Content-Type'] = 'text/csv'
            fname = 'Illinois_Sunshine_Committee_Expenditures_%s_%s.csv' % ('_'.join(committee_info['name'].split(' ')), filedate)
            response.headers['Content-Disposition'] = 'attachment; filename="%s"' % (fname)
            
            return response

        total_rows = base_query.count()
        
        resp['objects'] = objs
        resp['meta']['query'].update({
            'limit': limit,
            'offset': offset,
            'sort_order': sort_order,
            'order_by': order_by,
        })
        resp['meta']['total_rows'] = total_rows
    
    
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
    if 'datatype' in args_keys:
        args_keys.remove('datatype')
    if 'term' in args_keys:
        args_keys.remove('term')
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
