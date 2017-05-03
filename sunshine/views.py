from flask import Blueprint, render_template, abort, request, redirect, \
    session as flask_session, g
from sunshine.database import db_session
from sunshine.models import Candidate, Committee, Receipt, Expenditure
from sunshine.cache import cache, make_cache_key, CACHE_TIMEOUT
from sunshine.app_config import FLUSH_KEY
from sunshine import lib as sslib
import sqlalchemy as sa
from datetime import datetime, timedelta
from dateutil.parser import parse
import csv
import os
import collections

views = Blueprint('views', __name__)


@views.route('/')
@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
def index():

    # summary total text
    totals_sql = '''
      SELECT
        SUM(total_amount) as total_amount,
        SUM(donation_count) as donation_count,
        AVG(average_donation) as average_donation
      FROM receipts_by_month'''

    totals = list(g.engine.execute(sa.text(totals_sql)))

    # contested races chart
    top_races_sql = '''
        SELECT
          district,
          branch,
          SUM(total_money) AS money_sum
        FROM contested_races
        GROUP BY district, branch
        ORDER BY SUM(total_money) DESC
        LIMIT 10;
    '''

    races = list(g.engine.execute(sa.text(top_races_sql)))
    top_races = {}
    counter = 0
    maxc = 2
    for tr in races:

        district = tr.district
        branch = tr.branch

        candidates_sql = '''
            SELECT
              c.*
            FROM contested_races as c
            WHERE
              c.district = :district
              AND
                c.branch = :branch
        '''

        candidates = list(g.engine.execute(
            sa.text(candidates_sql), district=district, branch=branch)
        )
        if (branch == 'H'):
            branch = 'House'
        elif (branch == 'S'):
            branch = 'Senate'
        else:
            branch = 'Comptroller'
            district = 0

        if len(candidates) > maxc:
            maxc = len(candidates)
        cands = []
        for c in candidates:
            cand_name = c.first_name + " " + c.last_name
            if (cands and c.incumbent == 'N'):
                cands.append({
                    'candidate_id': c.candidate_id,
                    'party': c.party,
                    'incumbent': c.incumbent,
                    'name': cand_name
                })
            else:
                cands.insert(0, {
                    'candidate_id': c.candidate_id,
                    'party': c.party,
                    'incumbent': c.incumbent,
                    'name': cand_name
                })

        top_races[counter] = {
            'district': district,
            'branch': branch,
            'total_money': tr.money_sum,
            'candidates': cands
        }
        counter = counter + 1

    # donations chart
    donations_by_month_sql = '''
      SELECT * FROM receipts_by_month WHERE month >= :start_date
    '''

    donations_by_month = [
        [
            d.total_amount,
            d.month.year,
            d.month.month,
            d.month.day
        ] for d in g.engine.execute(
            sa.text(donations_by_month_sql),
            start_date="1994-01-01"
        )
    ]

    donations_by_year_sql = '''
      SELECT
        date_trunc('year', month) AS year,
        SUM(total_amount) AS total_amount,
        COUNT(donation_count) AS donation_count,
        AVG(average_donation) AS average_donation
      FROM receipts_by_month
      WHERE month >= :start_date
      GROUP BY date_trunc('year', month)
      ORDER BY year
    '''

    donations_by_year = [
        [
            d.total_amount,
            d.year.year,
            d.year.month,
            d.year.day
        ] for d in g.engine.execute(
            sa.text(donations_by_year_sql),
            start_date="1994-01-01"
        )
    ]

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
    top_earners = g.engine.execute(
        sa.text(top_earners),
        received_date=days_ago
    )

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

    top_ten = g.engine.execute(sa.text(committee_sql))

    date = datetime.now().date()

    days_donations_sql = '''
      SELECT
        c.*,
        cm.name as committee_name
      FROM condensed_receipts AS c
      JOIN committees AS cm ON c.committee_id = cm.id
      WHERE c.received_date >= :start_date
        AND c.received_date < :end_date
      ORDER BY c.amount DESC
      LIMIT 10
    '''

    days_donations = []

    # Roll back day until we find something
    while len(days_donations) == 0:
        days_donations = list(g.engine.execute(
            sa.text(days_donations_sql),
            start_date=(date - timedelta(days=7)),
            end_date=date
        ))
        date = date - timedelta(days=1)

    return render_template('index.html',
                           top_races=top_races,
                           maxc=maxc,
                           top_earners=top_earners,
                           top_ten=top_ten,
                           totals=totals,
                           donations_by_month=donations_by_month,
                           donations_by_year=donations_by_year,
                           days_donations=days_donations)


@views.route('/donations/')
@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
def donations():

    date = (datetime.utcnow() - timedelta(hours=6)).date() - timedelta(days=1)
    if request.args.get('date'):
        date = parse(request.args.get('date'))

    days_donations_sql = '''
      SELECT
        c.*,
        cm.name as committee_name
      FROM condensed_receipts AS c
      JOIN committees AS cm ON c.committee_id = cm.id
      WHERE c.received_date = :start_date
      ORDER BY c.received_date DESC
    '''

    days_donations = []

    # Roll back day until we find something
    while len(days_donations) == 0:
        days_donations = list(g.engine.execute(
            sa.text(days_donations_sql),
            start_date=date
        ))
        if days_donations:
            break

        date = date - timedelta(days=1)

    prev_day_date = date - timedelta(days=1)
    next_day_date = date + timedelta(days=1)
    is_current = (date == datetime.now().date())

    days_total_count = len(days_donations)
    days_total_donations = sum([d.amount for d in days_donations])

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


@views.route('/api-documentation/')
def api_documentation():
    return render_template('api-documentation.html')


@views.route('/error/')
def error():
    return render_template('error.html')


@views.route('/search/')
def search():
    term = request.args.get('term')
    table_name = request.args.getlist('table_name')
    search_date__le = request.args.get('search_date__le')
    search_date__ge = request.args.get('search_date__ge')
    if not table_name:
        table_name = [
            'candidates',
            'committees',
            'officers',
            'receipts',
            'expenditures'
        ]

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

    return render_template('candidate-detail.html',
                           candidate=candidate,
                           supporting=supporting)


@views.route('/top-earners/')
@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
def top_earners():

    days_ago = 30
    if request.args.get('days_ago'):
        try:
            days_ago = int(request.args.get('days_ago'))
        except:
            pass

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

    top_earners = g.engine.execute(
        sa.text(top_earners),
        received_date=calc_days_ago.strftime('%Y-%m-%d')
    )

    return render_template(
        'top-earners.html',
        top_earners=top_earners,
        days_ago=days_ago,
        calc_days_ago=calc_days_ago
    )


@views.route('/muni-contested-races/')
@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
def muni_contested_races():

    muni_contested_races_sql = '''
        SELECT *
        FROM muni_contested_races
    '''

    races = list(g.engine.execute(sa.text(muni_contested_races_sql)))

    contested_races_type = "Municipal"
    contested_races_title = "Illinois Municipal Contested Races"
    type_arg = 'municipal'

    contested_dict = {}
    total_money = {}
    for race in races:
        district = race.district

        if district in contested_dict:
            if race.incumbent == 'N':
                contested_dict[district].append({
                    'last': race.last_name,
                    'first': race.first_name,
                    'incumbent': race.incumbent,
                    'party': race.party,
                    'branch': race.branch
                })
            else:
                contested_dict[district].insert(0, {
                    'last': race.last_name,
                    'first': race.first_name,
                    'incumbent': race.incumbent,
                    'party': race.party,
                    'branch': race.branch
                })

        else:
            contested_dict[district] = []
            contested_dict[district].append({
                'last': race.last_name,
                'first': race.first_name,
                'incumbent': race.incumbent,
                'party': race.party,
                'branch': race.branch
            })

        if district in total_money:
            total_money[district] = total_money[district] + race.total_money
        else:
            total_money[district] = race.total_money

    sorted_x = sorted(total_money.items(), key=lambda x: x[1], reverse=True)

    total_money = collections.OrderedDict(sorted_x)
    sorted_contested = []
    for district in total_money.keys():
        sorted_contested.append((district, contested_dict[district]))

    contested_dict = collections.OrderedDict(sorted_contested)

    if not flask_session.get('%s_page_count' % type_arg):

        page_count = int(round(len(contested_dict), -2) / 50)

        flask_session['%s_page_count' % type_arg] = page_count

    else:

        page_count = flask_session['%s_page_count' % type_arg]
    return render_template('muni-contested-races.html',
                           contested_dict=contested_dict,
                           total_money=total_money,
                           contested_races_type=contested_races_type,
                           contested_races_title=contested_races_title,
                           page_count=page_count)


@views.route('/muni-contested-race-detail/<district>/')
@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
def muni_contested_race_detail(district):

    contested_race_title = district + " - Contested Race "

    contested_race_description = \
        "Contested race information for the " + district

    muni_contested_races_sql = '''
        SELECT *
        FROM muni_contested_races
        WHERE district = :district
    '''

    races = list(g.engine.execute(
        sa.text(muni_contested_races_sql),
        district=district
    ))

    contested_races = []
    for race in races:
        if race.incumbent == 'N':
            contested_races.append({
                'last': race.last_name,
                'first': race.first_name,
                'committee_name': race.committee_name,
                'incumbent': race.incumbent,
                'committee_id': race.committee_id,
                'party': race.party,
                'investments': race.investments,
                'debts': race.debts,
                'supporting_funds': race.supporting_funds,
                'opposing_funds': race.opposing_funds,
                'contributions': race.contributions,
                'total_funds': race.total_funds,
                'funds_available': race.funds_available,
                'total_money': race.total_money,
                'reporting_period_end': race.reporting_period_end
            })
        else:
            contested_races.insert(0, {
                'last': race.last_name,
                'first': race.first_name,
                'committee_name': race.committee_name,
                'incumbent': race.incumbent,
                'committee_id': race.committee_id,
                'party': race.party,
                'investments': race.investments,
                'debts': race.debts,
                'supporting_funds': race.supporting_funds,
                'opposing_funds': race.opposing_funds,
                'contributions': race.contributions,
                'total_funds': race.total_funds,
                'funds_available': race.funds_available,
                'total_money': race.total_money,
                'reporting_period_end': race.reporting_period_end
            })
    total_money = 0
    for c in contested_races:
            total_money += c['total_money']

    return render_template(
        'muni-contested-race-detail.html',
        district=district,
        contested_race_title=contested_race_title,
        contested_race_description=contested_race_description,
        contested_races=contested_races,
        total_money=total_money
    )


@views.route('/contested-races/')
@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
def contested_races():

    contested_races_type = "House of Representatives"
    contested_races_title = \
        "Illinois House of Representatives Contested Races"
    # TODO: Update the default argument here when the other pages are enabled again.
    type_arg = 'gubernatorial' if not request.args.get('type') else request.args.get('type', 'gubernatorial')

    is_house = (type_arg == "house_of_representatives")
    is_senate = (type_arg == "senate")
    is_comptroller = (type_arg == "comptroller")
    is_gubernatorial = (type_arg == "gubernatorial")
    input_filename = "contested_races.csv"

    if is_senate:
        contested_races_type = "Senate"
        contested_races_title = "Illinois Senate Contested Races"
    elif is_comptroller:
        contested_races_type = "State Comptroller"
        contested_races_title = "Illinois State Comptroller Contested Race"
        input_filename = "comptroller_contested_race.csv"
    elif is_gubernatorial:
        contested_races_type = "Gubernatorial"
        contested_races_title = "Race for Illinois Governor"
        input_filename = "gubernatorial_contested_races.csv"

    contested_races = list(csv.DictReader(open(
        os.getcwd() + '/sunshine/' + input_filename
    )))

    if is_house or is_senate:
        race_sig = "H" if is_house else "S"

        contested_races = filter(
            lambda race: race['Senate/House'] == race_sig,
            contested_races
        )

    contested_dict = {}
    cand_span = 0

    for e in contested_races:
        if is_comptroller or is_gubernatorial:
            district = 0
        else:
            district = int(e['District'])

        first_names = e['First'].split(';')
        last_names = e['Last'].split(';')

        first_name = first_names[0].strip()
        last_name = last_names[0].strip()

        candidate_data = {'last': last_name, 'first': first_name,'incumbent': e['Incumbent'],'party': e['Party']}

        if district in contested_dict:
            if e['Incumbent'] == 'N':
                contested_dict[district].append(candidate_data)
            else:
                contested_dict[district].insert(0, candidate_data)
        else:
            contested_dict[district] = []
            contested_dict[district].append(candidate_data)

        district_candidates = len(contested_dict[district])
        cand_span = cand_span if cand_span >= district_candidates else district_candidates

    if not flask_session.get('%s_page_count' % type_arg):

        page_count = int(round(len(contested_dict), -2) / 50)

        flask_session['%s_page_count' % type_arg] = page_count

    else:

        page_count = flask_session['%s_page_count' % type_arg]


    return render_template('contested-races.html',
                           cand_span=cand_span,
                           contested_dict=contested_dict,
                           contested_races_type=contested_races_type,
                           contested_races_title=contested_races_title,
                           page_count=page_count)






@views.route('/contested-race-detail/<race_type>-<district>/')
@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
def contested_race_detail(race_type, district):
    pre_primary_start = "2017-01-01"
    primary_start = "2018-01-01"
    primary_end = "2018-03-20"
    primary_quarterly_end = "2018-03-31"
    post_primary_start = (parse(primary_end) + timedelta(days=1)).strftime("%Y-%m-%d")
    is_after_primary = parse(primary_end).date() < datetime.today().date()

    contested_race_type = ""
    contested_race_title = ""
    branch = ""
    contested_race_description = ""
    template = 'contested-race-detail.html'

    if race_type == 'comptroller':
        contested_race_type = "State Comptroller"
        contested_race_title = "State Comptroller - Contested Race"
        branch = 'C'
        contested_race_description = "Contested race information for Illinois State Comptroller"
    elif race_type == "gubernatorial":
        contested_race_type = "State Gubernatorial"
        contested_race_title = "State Gubernatorial - Contested Race"
        branch = 'G'
        contested_race_description = "Contested race information for Illinois State Gubernatorial"
    elif race_type == "house":
        contested_race_type = "House of Representatives"
        contested_race_title = "District " + district + " - Contested Race "
        branch = 'H'
        contested_race_description = "Contested race information for District " + district + " in the Illinois House of Representatives"
    elif race_type == "senate":
        contested_race_type = "Senate"
        contested_race_title = "District " + district + " - Contested Race "
        branch = 'S'
        contested_race_description = "Contested race information for District " + district + " in the Illinois Senate"

    district = int(float(district))

    contested_races_sql = '''
        SELECT *
        FROM contested_races
        WHERE district = :district
          AND branch = :branch
    '''

    races = list(g.engine.execute(sa.text(contested_races_sql),district=district,branch=branch))

    total_money = 0.0
    contested_races = []
    for race in races:
        committee_funds_data = sslib.getCommitteeFundsData(race.committee_id, pre_primary_start, primary_start, post_primary_start)
        primary_funds_raised = sslib.getFundsRaisedTotal(race.committee_id, pre_primary_start, primary_start, primary_end) if is_after_primary else None
        total_money += (committee_funds_data[-1][1] if committee_funds_data else 0.0)
        total_money += race.supporting_funds + race.opposing_funds

        if race.incumbent == 'N':
            contested_races.append({'table_display_data': committee_funds_data, 'primary_funds_raised': primary_funds_raised, 'last': race.last_name, 'first': race.first_name,'committee_name': race.committee_name,'incumbent': race.incumbent,'committee_id': race.committee_id,'party': race.party,'investments': race.investments, 'debts': race.debts, 'supporting_funds': race.supporting_funds, 'opposing_funds': race.opposing_funds, 'contributions' : race.contributions, 'total_funds' : race.total_funds, 'funds_available' : race.funds_available, 'total_money' : race.total_money, 'candidate_id' : race.candidate_id, 'reporting_period_end' : race.reporting_period_end})
        else:
            contested_races.insert(0,{'table_display_data': committee_funds_data, 'primary_funds_raised': primary_funds_raised, 'last': race.last_name, 'first': race.first_name,'committee_name': race.committee_name,'incumbent': race.incumbent,'committee_id': race.committee_id,'party': race.party,'investments': race.investments, 'debts': race.debts, 'supporting_funds': race.supporting_funds, 'opposing_funds': race.opposing_funds, 'contributions' : race.contributions, 'total_funds' : race.total_funds, 'funds_available' : race.funds_available, 'total_money' : race.total_money, 'candidate_id' : race.candidate_id, 'reporting_period_end' : race.reporting_period_end})

    return render_template(template,
                            race_type=race_type,
                            district=district,
                            contested_race_type=contested_race_type,
                            contested_race_title=contested_race_title,
                            contested_race_description=contested_race_description,
                            contested_races=contested_races,
                            total_money=total_money)


@views.route('/committees/')
@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
def committees():

    committee_type = "Candidate"
    committee_title = "Candidate Committees"
    type_arg = 'candidate'

    if request.args.get('type'):
      type_arg = request.args.get('type', 'candidate')
      if type_arg == "super_pac":
        committee_type = "Super PAC"
        committee_title = "Super PACs"
      if type_arg == "action":
        committee_type = "Political Action"
        committee_title = "Political Action Committees"
      if type_arg == "party":
        committee_type = "Political Party"
        committee_title = "Political Party Committees"
      if type_arg == "ballot":
        committee_type = "Ballot Initiative"
        committee_title = "Ballot Initiative Committees"

    page = request.args.get('page', 1)
    offset = (int(page) * 50) - 50

    if committee_type == "Candidate":
      sql = '''
          SELECT * FROM (
            SELECT distinct ON (committee_id)
            committee_money.*, candidates.*
            FROM committee_money
            LEFT JOIN candidate_committees ON committee_money.committee_id = candidate_committees.committee_id
            LEFT JOIN candidates ON candidates.id = candidate_committees.candidate_id
            WHERE committee_type = :committee_type
            ORDER BY committee_id, committee_name
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

    if not flask_session.get('%s_page_count' % type_arg):

        result_count = '''
            SELECT COUNT(*) AS count
            FROM committee_money
            WHERE committee_type = :committee_type
        '''

        result_count = g.engine.execute(sa.text(result_count),
                                              committee_type=committee_type)\
                                              .first()\
                                              .count

        page_count = int(round(result_count, -2) / 50)

        flask_session['%s_page_count' % type_arg] = page_count

    else:

        page_count = flask_session['%s_page_count' % type_arg]

    committees = g.engine.execute(sa.text(sql),
                                committee_type=committee_type,
                                offset=offset)
    return render_template('committees.html',
                           committees=committees,
                           committee_type=committee_type,
                           committee_title=committee_title,
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

    latest_filing = '''
        SELECT * FROM most_recent_filings
        WHERE committee_id = :committee_id
        ORDER BY received_datetime DESC
        LIMIT 1
    '''

    latest_filing = dict(g.engine.execute(sa.text(latest_filing),
                                   committee_id=committee_id).first())

    params = {'committee_id': committee_id}

    if not latest_filing['reporting_period_end']:
      latest_filing['reporting_period_end'] = datetime.now().date() - timedelta(days=90)


    if latest_filing['end_funds_available'] \
        or latest_filing['end_funds_available'] == 0:

        recent_receipts = '''
            SELECT
              COALESCE(SUM(receipts.amount), 0) AS amount
            FROM condensed_receipts AS receipts
            JOIN filed_docs AS filed
              ON receipts.filed_doc_id = filed.id
            WHERE receipts.committee_id = :committee_id
              AND receipts.received_date > :end_date
        '''
        controlled_amount = latest_filing['end_funds_available']

        params['end_date'] = latest_filing['reporting_period_end']

    else:

        recent_receipts = '''
            SELECT
              COALESCE(SUM(receipts.amount), 0) AS amount
            FROM condensed_receipts AS receipts
            JOIN filed_docs AS filed
              ON receipts.filed_doc_id = filed.id
            WHERE receipts.committee_id = :committee_id
        '''

        controlled_amount = 0

    recent_total = g.engine.execute(sa.text(recent_receipts),**params).first().amount
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
              oc.committee_id,
              o.first_name,
              o.last_name,
              cm.type,
              m.total AS money
            FROM committees AS cm
            LEFT JOIN officer_committees AS oc
              ON cm.id = oc.committee_id
            JOIN officers AS o
              ON oc.officer_id = o.id
            LEFT JOIN committee_money AS m
              ON oc.committee_id = m.committee_id
          ) AS o
          JOIN (
            SELECT
              cm.name,
              oc.committee_id,
              o.first_name,
              o.last_name,
              cm.type,
              m.total AS money
            FROM committees AS cm
            LEFT JOIN officer_committees AS oc
              ON cm.id = oc.committee_id
            JOIN officers AS o
              ON oc.officer_id = o.id
            LEFT JOIN committee_money AS m
              ON oc.committee_id = m.committee_id
          ) AS o2
            ON TRIM(BOTH ' ' FROM REPLACE(o.first_name, '.', '')) = TRIM(BOTH ' ' FROM REPLACE(o2.first_name, '.', ''))
            AND TRIM(BOTH ' ' FROM REPLACE(o.last_name, '.', '')) = TRIM(BOTH ' ' FROM REPLACE(o2.last_name, '.', ''))
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
                 c.name,
                 c.id,
                 c.type,
                 c.active,
                 m.total AS money,
                 'Officer with same name as supported candidate' AS reason
               FROM candidates AS cd
               LEFT JOIN officers AS o
                 ON TRIM(BOTH ' ' FROM REPLACE(cd.first_name, '.', '')) = TRIM(BOTH ' ' FROM REPLACE(o.first_name, '.', ''))
                 AND TRIM(BOTH ' ' FROM REPLACE(cd.last_name, '.', '')) = TRIM(BOTH ' ' FROM REPLACE(o.last_name, '.', ''))
               LEFT JOIN officer_committees AS oc
                 ON o.id = oc.officer_id
               JOIN committees AS c
                 ON oc.committee_id = c.id
               LEFT JOIN committee_money AS m
                 ON oc.committee_id = m.committee_id
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

    related_committees = list(g.engine.execute(sa.text(related_committees),**params))

    supported_candidates = []
    opposed_candidates = []

    related_candidates_sql = '''
        SELECT
          candidate_name,
          office,
          opposing,
          supporting,
          supporting_amount,
          opposing_amount
        FROM expenditures_by_candidate
        WHERE committee_id = :committee_id
        ORDER BY supporting_amount DESC, opposing_amount DESC
    '''

    related_candidates = list(g.engine.execute(sa.text(related_candidates_sql),
                                        committee_id=committee.id))
    for c in related_candidates:
        if c.supporting:
            added = False
            for sc in supported_candidates:
                if sc['candidate_name'] == c.candidate_name:
                    added = True
                    sc['office'] = sc['office'] + ", " + c.office
            if not added:
                supported_candidates.append(dict(c.items()))

        if c.opposing:
            added = False
            for sc in opposed_candidates:
                if sc['candidate_name'] == c.candidate_name:
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
          AND f.reporting_period_end IS NOT NULL
          AND f.doc_name = 'Quarterly'
        ORDER BY f.reporting_period_end ASC
    '''

    quarterlies = list(g.engine.execute(sa.text(quarterlies),
                                 committee_id=committee_id))

    expended_date = latest_filing['reporting_period_end']

    recent_expenditures_sql = '''
        SELECT
          (amount * -1) AS amount,
          expended_date
        FROM condensed_expenditures
        WHERE expended_date > :expended_date
          AND committee_id = :committee_id
        ORDER BY expended_date DESC
    '''

    recent_expenditures = list(g.engine.execute(sa.text(recent_expenditures_sql),
                                        expended_date=expended_date,
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

    expenditures = []
    for r in recent_expenditures:
        expenditures.append([r.amount, r.expended_date.year, r.expended_date.month, r.expended_date.day])


    for r in quarterlies:
        expenditures.append([r.total_expenditures,
                     r.reporting_period_end.year,
                     r.reporting_period_end.month,
                     r.reporting_period_end.day])

    total_donations = sum([r.total_receipts for r in quarterlies])
    total_expenditures = sum([r.total_expenditures for r in quarterlies]) + sum([r.amount for r in recent_expenditures])
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

@views.route('/independent-expenditures/<candidate_id>-<stance>/')
@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
def independent_expenditures(candidate_id, stance):


    candidate_id = candidate_id.rsplit('-', 1)[-1]

    try:
        candidate_id = int(candidate_id)
    except ValueError:
        return abort(404)

    candidate = db_session.query(Candidate).get(candidate_id)

    if not candidate:
        return abort(404)

    cname = [candidate.first_name, candidate.last_name]
    candidate_name = " ".join(cname)

    independent_expenditures_type = 'Supporting'
    independent_expenditures_title = "Supporting Independent Expenditures"
    independent_expenditures_description = "Independent expenditures in support of " + candidate_name + " since March 16, 2016"
    type_arg = 'supporting'

    if stance == 'opposing':
        independent_expenditures_type = 'Opposing'
        independent_expenditures_title = "Opposing Independent Expenditures"
        independent_expenditures_description = "Independent expenditures in opposition to " + candidate_name + " since March 16, 2016"

    all_names = []
    all_names.append(candidate_name)

    #get all possible names for candidate
    alternate_names_sql = '''
        SELECT
          alternate_names
        FROM contested_races
        WHERE candidate_id = :candidate_id
    '''

    alternate_names = g.engine.execute(sa.text(alternate_names_sql), candidate_id=candidate_id).first()
    if alternate_names:
        alt_names = alternate_names.alternate_names.split(';')

        for name in alt_names:
            all_names.append(name.strip())

    d2_part = '9B'
    expended_date = datetime(2016, 3, 16, 0, 0)

    independent_expenditures =[]
    master_names = set(all_names)
    for candidate_name in master_names:

        if independent_expenditures_type == "Supporting":

            ind_expenditures_sql = '''
                SELECT
                  committee_id,
                  amount AS amount,
                  expended_date AS date
                FROM condensed_expenditures
                WHERE candidate_name = :candidate_name
                  AND d2_part = :d2_part
                  AND expended_date > :expended_date
                  AND supporting = 'true'
            '''

        else:
            ind_expenditures_sql = '''
                SELECT
                  committee_id,
                  amount AS amount,
                  expended_date AS date
                FROM condensed_expenditures
                WHERE candidate_name = :candidate_name
                  AND d2_part = :d2_part
                  AND expended_date > :expended_date
                  AND opposing = 'true'
            '''

        ind_expends = list(g.engine.execute(sa.text(ind_expenditures_sql),
                                            candidate_name=candidate_name,
                                            d2_part=d2_part,
                                            expended_date=expended_date))
        for ie in ind_expends:
            ie_dict = dict(ie.items())
            ie_dict['committee_name'] = db_session.query(Committee).get(ie_dict['committee_id']).name
            independent_expenditures.append(ie_dict)


    newlist = sorted(independent_expenditures, key=lambda k: k['date'])
    candidate_name = " ".join(cname)

    return render_template('independent-expenditures.html',
                            independent_expenditures_type=independent_expenditures_type,
                            independent_expenditures_title=independent_expenditures_title,
                            independent_expenditures=newlist,
                            independent_expenditures_description=independent_expenditures_description,
                            candidate_name=candidate_name,
                            candidate_id=candidate_id)

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
    if not receipt.committee:
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
    if not expense.committee:
        return abort(404)
    return render_template('expense-detail.html', expense=expense)

# Widgets
@views.route('/widgets/top-earners/')
@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
def widget_top_earners():
    days_ago = 30
    if request.args.get('days_ago'):
      try:
        days_ago = int(request.args.get('days_ago'))
      except:
        pass

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
        LIMIT 5
    '''

    calc_days_ago = datetime.now() - timedelta(days=days_ago)

    top_earners = g.engine.execute(sa.text(top_earners),
                                 received_date=calc_days_ago.strftime('%Y-%m-%d'))

    return render_template('widgets/top-earners.html',
                            top_earners=top_earners,
                            days_ago=days_ago,
                            calc_days_ago=calc_days_ago)


@views.route('/widgets/top-donations/')
@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
def widgets_top_donations():

    start_date = datetime.now().date()
    end_date = start_date
    days_donations_sql = '''
      SELECT
        c.*,
        cm.name as committee_name
      FROM condensed_receipts AS c
      JOIN committees AS cm ON c.committee_id = cm.id
      WHERE c.received_date >= :start_date
        AND c.received_date < :end_date
      ORDER BY c.amount DESC
      LIMIT 10
    '''

    days_donations = []

    # Roll back day until we find something
    while len(days_donations) == 0:
        start_date = start_date - timedelta(days=7)
        days_donations = list(g.engine.execute(sa.text(days_donations_sql),
                                             start_date=start_date,
                                             end_date=end_date))

    return render_template('widgets/top-donations.html',
                           days_donations=days_donations)


@views.route('/widgets/top-contested-races/')
@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
def widgets_top_contested_races():

    top_races_sql = '''
        SELECT
          district,
          branch,
          SUM(total_money) AS money_sum
        FROM contested_races
        GROUP BY district, branch
        ORDER BY SUM(total_money) DESC
        LIMIT 10;
    '''

    races = list(g.engine.execute(sa.text(top_races_sql)))
    top_races = {}
    counter = 0
    maxc = 2
    for tr in races:

        district = tr.district
        branch = tr.branch

        candidates_sql = '''
            SELECT
              c.*
            FROM contested_races as c
            WHERE
              c.district = :district
              AND
                c.branch = :branch
        '''

        candidates = list(g.engine.execute(sa.text(candidates_sql),district=district,branch=branch))
        if (branch == 'H'):
            branch = 'House'
        elif (branch == 'S'):
            branch = 'Senate'
        else:
            branch = 'Comptroller'
            district = 0

        if len(candidates) > maxc:
            maxc = len(candidates)
        cands=[]
        for c in candidates:
            cand_name = c.first_name + " " + c.last_name
            if (cands and c.incumbent == 'N'):
                cands.append({'candidate_id': c.candidate_id,'party': c.party, 'incumbent': c.incumbent,'name': cand_name})
            else:
                cands.insert(0,{'candidate_id': c.candidate_id,'party': c.party, 'incumbent': c.incumbent,'name': cand_name})

        top_races[counter] = {'district': district, 'branch': branch, 'total_money': tr.money_sum, 'candidates': cands}
        counter = counter+1


    return render_template('widgets/top-contested-races.html',
                           top_races=top_races,maxc=maxc)



@views.route('/flush-cache/<secret>/')
def flush(secret):
    if secret == FLUSH_KEY:
        cache.clear()
    return 'Woop'

@views.route('/sunshine/')
def sunshine():
    return redirect("/", code=301)

@views.route('/developers/')
def developers():
    return redirect("/api-documentation/", code=301)

@views.route('/sunshine/<path:the_rest>/')
def sunshine_the_rest(the_rest):
    return redirect("/", code=301)
