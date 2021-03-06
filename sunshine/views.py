from flask import Blueprint, render_template, abort, request, redirect, \
    session as flask_session, g, current_app, flash
from flask_login import login_required, login_user, logout_user, \
    current_user
from sunshine.database import db_session
from sunshine.models import Candidate, Committee, Receipt, Expenditure, \
    User
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

    # contested races funds for house/senate
    house_senate_count = getHouseSenateContestedRacesCount()

    # democratic committee funds
    democratic_sql = '''
        SELECT * FROM (
          SELECT *
          FROM committee_money
          WHERE committee_active = TRUE
          AND committee_id IN (6239, 23189, 21318, 665, 124)
          ORDER BY committee_name
        ) AS committees
        ORDER BY committees.total DESC NULLS LAST
    '''

    democratic_committees = g.engine.execute(sa.text(democratic_sql))

    # republican committee funds
    republican_sql = '''
        SELECT * FROM (
          SELECT *
          FROM committee_money
          WHERE committee_active = TRUE
          AND committee_id IN (292, 17218, 17589, 7516, 7537, 25185)
          ORDER BY committee_name
        ) AS committees
        ORDER BY committees.total DESC NULLS LAST
    '''

    republican_committees = g.engine.execute(sa.text(republican_sql))

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

    # News Content sql
    news_sql = """SELECT key, content FROM news_table ORDER BY key"""
    news_content = list(g.engine.execute(sa.text(news_sql)))

    if (not news_content):
        news_content = [
            {'key': '1', 'content': ''},
            {'key': '2', 'content': ''},
            {'key': '3', 'content': ''}
        ]

    return render_template('index.html',
                           # top_races=top_races,
                           # maxc=maxc,
                           top_earners=top_earners,
                           top_ten=top_ten,
                           democratic_committees=democratic_committees,
                           republican_committees=republican_committees,
                           totals=totals,
                           donations_by_month=donations_by_month,
                           donations_by_year=donations_by_year,
                           days_donations=days_donations,
                           news_content=news_content,
                           house_senate_count=house_senate_count)


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

    type_arg = 'house_of_representatives' if not request.args.get('type') else request.args.get('type', 'house_of_representatives')
    visible = request.args.get('visible')

    if type_arg == "gubernatorial":
        return render_template('gov-contested-race-details.html', contested_races_type=type_arg)

    cr_type, cr_title, contested_race_info = sslib.getContestedRacesInformation(type_arg)

    return render_template('contested-races.html',
                            contested_races_type=cr_type,
                            contested_races_title=cr_title,
                            contested_race_info=contested_race_info,
                            visible=visible)


@views.route('/federal-races/')
@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
def federal_races():

    type_arg = 'house_of_representatives' if not request.args.get('type') else request.args.get('type', 'house_of_representatives')
    visible = request.args.get('visible')

    cr_type, cr_title, contested_race_info = sslib.getFederalRacesInformation(type_arg)

    description = ""

    if cr_type == "House of Representatives":
        description = "2018 Races for Seats in the US House of Representatives."
    elif cr_type == "Senate":
        description = "Includes individuals filed with the FEC as candidates for the 2020 Senate election."
    elif cr_type == "President":
        description = "Includes individuals filed with the FEC as candidates for the 2020 Presidential election."

    return render_template('federal-races.html',
                            description=description,
                            contested_races_type=cr_type,
                            contested_races_title=cr_title,
                            contested_race_info=contested_race_info,
                            visible=visible)


@views.route('/contested-race-detail/<race_type>-<district>/')
@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
def contested_race_detail(race_type, district):
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

    primary_details = sslib.getPrimaryDetails(branch)
    pre_primary_start = primary_details["pre_primary_start"]
    primary_start = primary_details["primary_start"]
    primary_end = primary_details["primary_end"]
    primary_quarterly_end = primary_details["primary_quarterly_end"]
    post_primary_start = primary_details["post_primary_start"]
    is_after_primary = primary_details["is_after_primary"]

    contested_races_sql = '''
        SELECT cr.*
        FROM contested_races cr
        WHERE cr.district = :district
          AND cr.branch = :branch
        ORDER BY cr.last_name, cr.first_name
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
            contested_races.append({'table_display_data': committee_funds_data, 'primary_funds_raised': primary_funds_raised, 'last': race.last_name, 'first': race.first_name,'committee_name': race.committee_name if race.committee_name else '', 'incumbent': race.incumbent,'committee_id': race.committee_id,'party': race.party,'investments': race.investments, 'debts': race.debts, 'supporting_funds': race.supporting_funds, 'opposing_funds': race.opposing_funds, 'contributions' : race.contributions, 'total_funds' : race.total_funds, 'funds_available' : race.funds_available, 'total_money' : race.total_money, 'candidate_id' : race.candidate_id, 'reporting_period_end' : race.reporting_period_end})
        else:
            contested_races.insert(0,{'table_display_data': committee_funds_data, 'primary_funds_raised': primary_funds_raised, 'last': race.last_name, 'first': race.first_name,'committee_name': race.committee_name if race.committee_name else '', 'incumbent': race.incumbent,'committee_id': race.committee_id,'party': race.party,'investments': race.investments, 'debts': race.debts, 'supporting_funds': race.supporting_funds, 'opposing_funds': race.opposing_funds, 'contributions' : race.contributions, 'total_funds' : race.total_funds, 'funds_available' : race.funds_available, 'total_money' : race.total_money, 'candidate_id' : race.candidate_id, 'reporting_period_end' : race.reporting_period_end})

    return render_template(template,
                            race_type=race_type,
                            district=district,
                            current_date=datetime.now(),
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

    latest_filing, recent_total, controlled_amount, params = sslib.getCommitteeRecentFilingData(committee_id)

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

    ie_popover_span = "<span class='ss-popover' data-content='Independent expenditures are ad buys made supporting or opposing candidates, without any collaboration or coordination with the candidate. These ad buys are often made by Super PACs or Party Committees.'>Independent expenditures</span>"
    ie_popunder_span = "<span class='ss-popunder' data-content='Independent expenditures are ad buys made supporting or opposing candidates, without any collaboration or coordination with the candidate. These ad buys are often made by Super PACs or Party Committees.'>Independent Expenditures</span>"

    independent_expenditures_type = 'Supporting'
    independent_expenditures_title = "Supporting " + ie_popunder_span
    independent_expenditures_description = ie_popover_span + " in support of " + candidate_name + " since March 16, 2016"
    type_arg = 'supporting'

    if stance == 'opposing':
        independent_expenditures_type = 'Opposing'
        independent_expenditures_title = "Opposing " + ie_popunder_span
        independent_expenditures_description = ie_popover_span + " in opposition to " + candidate_name + " since March 16, 2016"

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


@views.route('/widgets/gov-contested-race')
@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
def widgets_gov_contested_race():

    hideHeaders = request.args.get('hideHeaders')
    showImageString = request.args.get('showImage')
    showImage = showImageString == "True"

    contested_race_data = sslib.getAllCandidateFunds('0', "G")
    current_date = datetime.now()

    return render_template('widgets/gov-contested-race.html', current_date=current_date,
                           contested_race_data=contested_race_data, hideHeaders=hideHeaders,
                           showImage=showImage)


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

@views.route('/admin/login/', methods=['GET', 'POST'])
def admin_login():
    invalid = "Incorrect username or password"

    if request.method != 'POST':
        return render_template('admin/login.html')

    # Add logic that will check to see if the user exists
    # and verify hashed password is correct, if true direct to dashboard
    username = request.form.get("username")
    password = request.form.get("password")

    user = User.validate(username, password)

    if user is None:
        return render_template('admin/login.html', invalid=invalid)

    login_user(user, request.form.get("rememberMe"))
    return redirect('/admin/dashboard/')


@views.route('/admin/dashboard/')
@login_required
def admin_dashboard():
    return render_template('admin/dashboard.html')


@views.route('/admin/news/', methods=['GET', 'POST'])
@login_required
def admin_news():
    news_content = { '1': '', '2': '', '3': '' }

    if request.method != 'POST':
        exist_sql = "select key, content from news_table"
        news_records = list(g.engine.execute(sa.text(exist_sql)))

        for record in news_records:
            news_content[record["key"]] = record["content"]

        return render_template('admin/news.html', news_content=news_content)

    for key in news_content:
        news_content[key] = request.form.get("editor" + key)

        try:
            sql_params = {
                'key': key,
                'content': news_content[key]
            }

            exist_sql = """select * from news_table where key = :key"""
            exists = len(list(g.engine.execute(sa.text(exist_sql), **sql_params))) > 0

            if not exists:
                sql = """INSERT INTO news_table (key, content, created_date, updated_date) VALUES (:key, :content, now(), now())"""
                g.engine.execute(sa.sql.text(sql), **sql_params)
            else:
                sql = """UPDATE news_table SET content = :content, updated_date = now() WHERE key = :key"""
                g.engine.execute(sa.sql.text(sql), **sql_params)

            cache.clear()

            update_message = 'Saved successfully.'

        except sa.exc.ProgrammingError as e:
            update_message = "Save failed: " + str(e)
            current_app.logger.error(update_message)

    return render_template('admin/news.html', code=update_message, news_content=news_content)


@views.route('/admin/contested-races/')
@login_required
def admin_contested_races():

    type_arg = 'house_of_representatives' if not request.args.get('type') \
        else request.args.get('type', 'house_of_representatives')

    if type_arg == "senate":
        contested_races_type = "Senate"
        contested_races_title = "Illinois Senate Races"
        branch = "S"
    elif type_arg == "statewide_office":
        contested_races_type = "Statewide Offices"
        contested_races_title = "Illinois Constitutional Officer Races"
        branch = "O"
    elif type_arg == "house_of_representatives":
        contested_races_type = "House of Representatives"
        contested_races_title = "Illinois House of Representatives Races"
        branch = "H"
    else:
        contested_races_type = "Gubernatorial"
        contested_races_title = "Illinois Gubernatorial Races"
        branch = "G"

    contested_race_sql = '''
        SELECT *
        FROM contested_races
        WHERE branch = :branch

    '''

    if branch in ["H", "S"]:
        contested_race_sql += " ORDER BY district, party, last_name, first_name"
    else:
        contested_race_sql += " ORDER BY district_name, party, last_name, first_name"

    contested_race_data = list(g.engine.execute(sa.text(contested_race_sql),
                                                branch=branch))

    return render_template('admin/contested-races.html',
                           contested_races_type=contested_races_type,
                           contested_races_title=contested_races_title,
                           contested_race_data=contested_race_data)


@views.route('/admin/federal-races/')
@login_required
def admin_federal_races():

    type_arg = 'president' if not request.args.get('type') \
        else request.args.get('type', 'president')

    if type_arg == "senate":
        contested_races_type = "Senate"
        contested_races_title = "Illinois US Senate Races"
        branch = "S"
    elif type_arg == "house_of_representatives":
        contested_races_type = "House of Representatives"
        contested_races_title = "Illinois US House of Representatives Races"
        branch = "H"
    else:
        contested_races_type = "President"
        contested_races_title = "US Presidential Race"
        branch = "P"

    contested_race_sql = '''
        SELECT *
        FROM federal_races
        WHERE branch = :branch

    '''

    if branch == "H":
        contested_race_sql += " ORDER BY district, incumbent, name"
    else:
        contested_race_sql += " ORDER BY incumbent, name"

    contested_race_data = list(g.engine.execute(sa.text(contested_race_sql),
                                                branch=branch))

    return render_template('admin/federal-races.html',
                           contested_races_type=contested_races_type,
                           contested_races_title=contested_races_title,
                           contested_race_data=contested_race_data)


@views.route('/admin/contested-race-delete/', methods=['POST'])
@login_required
def admin_contested_race_delete():
    id = request.form.get("id")

    if not id:
        return redirect('/admin/contested-races/')

    race = sslib.getContestedRace(id)
    race_types = {
        'H': 'house_of_representatives',
        'S': 'senate',
        'O': 'statewide_office',
        'G': 'gubernatorial'
    }

    sslib.deleteContestedRace(id)

    cache.clear()

    if not race or not race.branch or not race_types.get(race.branch):
        return redirect('/admin/contested-races/')

    return redirect('/admin/contested-races/?type=' + race_types.get(race.branch))


@views.route('/admin/federal-race-delete/', methods=['POST'])
@login_required
def admin_federal_race_delete():
    id = request.form.get("id")

    if not id:
        return redirect('/admin/federal-races/')

    race = sslib.getFederalRace(id)
    race_types = {
        'H': 'house_of_representatives',
        'S': 'senate',
        'P': 'president',
    }

    sslib.deleteFederalRace(id)

    cache.clear()

    if not race or not race.branch or not race_types.get(race.branch):
        return redirect('/admin/federal-races/')

    return redirect('/admin/federal-races/?type=' + race_types.get(race.branch))


@views.route('/admin/contested-race-detail/', methods=['GET', 'POST'])
@login_required
def admin_contested_race_details():

    race_types = {
        'H': 'house_of_representatives',
        'S': 'senate',
        'O': 'statewide_office',
        'G': 'gubernatorial'
    }
    id = None if (not request.args.get("id") or request.args.get("id") == "None") else request.args.get("id")

    if not id:
        id = None if (not request.form.get("id") or request.form.get("id") == "None") else request.form.get("id")

    new_candidate = True if id is None else False
    statewide_districts = ['Attorney General', 'Comptroller', 'Secretary of State', 'Treasurer']

    if request.method != 'POST':
        info = getCandidateInfo(id)
        return render_template('admin/contested-race-detail.html',
                               id=id,
                               new_candidate=new_candidate,
                               candidate=info,
                               statewide_districts=statewide_districts)

    # Handle the cancel button.
    if request.form.get("cancel"):
        if not id:
            return redirect('/admin/contested-races/')

        race = sslib.getContestedRace(id)
        if not race:
            return redirect('/admin/contested-races/')

        return redirect('/admin/contested-races/?type=' + race_types.get(race.branch))

    candidate_info = {}
    candidate_info["branch"] = request.form.get("branch")
    candidate_info["district"] = 0 if candidate_info["branch"] not in ["H", "S"] else int(request.form.get("district"))
    candidate_info["district_name"] = request.form.get("district_name") if candidate_info["branch"] == "O" else str(request.form.get("district"))
    candidate_info["first_name"] = request.form.get("first_name")
    candidate_info["last_name"] = request.form.get("last_name")
    candidate_info["incumbent"] = 'N' if request.form.get("incumbent") is None else 'Y',
    candidate_info["committee_id"] = None if request.form.get("committee") == "" else request.form.get("committee")
    candidate_info["candidate_id"] = None if request.form.get("candidate") == "" else request.form.get("candidate")
    candidate_info["committee_name"] = getCommitteeName(candidate_info["committee_id"])
    candidate_info["party"] = request.form.get("party")
    candidate_info["alternate_names"] = str(request.form.get("alternate_names")).strip()

    id, messages = insertCandidate(id, candidate_info, statewide_districts)

    if not messages:
        flash('Candidate saved successfully', 'success')
    else:
        flash('<br />'.join(messages), 'error')

    cache.clear()

    return render_template('admin/contested-race-detail.html',
                           id=id,
                           new_candidate=False,
                           candidate=candidate_info,
                           committees=committees,
                           statewide_districts=statewide_districts)


@views.route('/admin/federal-race-detail/', methods=['GET', 'POST'])
@login_required
def admin_federal_race_details():

    race_types = {
        'H': 'house_of_representatives',
        'S': 'senate',
        'P': 'president'
    }
    id = None if (not request.args.get("id") or request.args.get("id") == "None") else request.args.get("id")

    if not id:
        id = None if (not request.form.get("id") or request.form.get("id") == "None") else request.form.get("id")

    new_candidate = True if id is None else False
    statewide_districts = ['Attorney General', 'Comptroller', 'Secretary of State', 'Treasurer']

    if request.method != 'POST':
        info = getFederalCandidateInfo(id)
        return render_template('admin/federal-race-detail.html',
                               id=id,
                               new_candidate=new_candidate,
                               candidate=info,
                               statewide_districts=statewide_districts)

    # Handle the cancel button.
    if request.form.get("cancel"):
        if not id:
            return redirect('/admin/federal-races/')

        race = sslib.getFederalRace(id)
        if not race:
            return redirect('/admin/federal-races/')

        return redirect('/admin/federal-races/?type=' + race_types.get(race.branch))

    candidate_info = {}
    candidate_info["branch"] = request.form.get("branch")
    candidate_info["district"] = 0 if candidate_info["branch"] != "H" else int(request.form.get("district"))
    candidate_info["name"] = request.form.get("name")
    candidate_info["incumbent"] = 'N' if request.form.get("incumbent") is None else 'Y'
    candidate_info["party"] = request.form.get("party")
    candidate_info["fec_link"] = str(request.form.get("fec_link")).strip()
    candidate_info["on_ballot"] = 'N' if request.form.get("on_ballot") is None else 'Y'

    id, messages = insertFederalCandidate(id, candidate_info)

    if not messages:
        flash('Candidate saved successfully', 'success')
    else:
        flash('<br />'.join(messages), 'error')

    cache.clear()

    return render_template('admin/federal-race-detail.html',
                           id=id,
                           new_candidate=False,
                           candidate=candidate_info,
                           committees=committees,
                           statewide_districts=statewide_districts)


@views.route('/admin/logout/')
def admin_logout():
    logout_user()
    return redirect('/admin/login/')


# helper methods
def getCandidateInfo(id):

    if id == "None":
        id = None

    info = {}
    info['branch'] = None
    info['last_name'] = None
    info['first_name'] = None
    info['committee_name'] = None
    info['incumbent'] = None
    info['committee_id'] = None
    info['party'] = None
    info['district'] = None
    info['district_name'] = None
    info['candidate_id'] = None
    info['alternate_names'] = None

    if id is not None:
        candidate_sql = '''SELECT * FROM contested_races WHERE id = :id'''
        candidate_info = g.engine.execute(sa.text(candidate_sql), id=id)

        for c in candidate_info:
            info['branch'] = c.branch
            info['last_name'] = c.last_name
            info['first_name'] = c.first_name
            info['committee_name'] = c.committee_name
            info['incumbent'] = c.incumbent
            info['committee_id'] = c.committee_id
            info['party'] = c.party
            info['district'] = c.district
            info['district_name'] = c.district_name
            info['candidate_id'] = c.candidate_id
            info['alternate_names'] = c.alternate_names

    return info


def getFederalCandidateInfo(id):

    if id == "None":
        id = None

    info = {}
    info['branch'] = None
    info['district'] = None
    info['name'] = None
    info['incumbent'] = None
    info['party'] = None
    info['fec_link'] = None
    info['on_ballot'] = None

    if id is not None:
        candidate_sql = '''SELECT * FROM federal_races WHERE id = :id'''
        candidate_info = g.engine.execute(sa.text(candidate_sql), id=id)

        for c in candidate_info:
            info['branch'] = c.branch
            info['district'] = c.district
            info['name'] = c.name
            info['incumbent'] = c.incumbent
            info['party'] = c.party
            info['fec_link'] = c.fec_link
            info['on_ballot'] = c.on_ballot

    return info


def getCommitteeName(committee_id):

    committee_name = ""

    # Negative value indicates no committee selected
    if committee_id is None:
        return committee_name

    committee_sql = '''SELECT name FROM committees WHERE id = :committee_id'''
    committee_list = list(g.engine.execute(sa.text(committee_sql), committee_id=committee_id))

    for c in committee_list:
        committee_name = c.name

    return committee_name

def isValidCommittee(committee_id):
    sql = '''SELECT count(*) as total FROM committees WHERE id = :committee_id'''
    total = list(g.engine.execute(sa.text(sql), committee_id=committee_id))
    return total and total[0]["total"] > 0

def isValidCandidate(candidate_id):
    sql = '''SELECT count(*) as total FROM candidates WHERE id = :candidate_id'''
    total = list(g.engine.execute(sa.text(sql), candidate_id=candidate_id))
    return total and total[0]["total"] > 0

def insertCandidate(id, candidate_info, statewide_districts):

    messages = []

    # Branch
    if not candidate_info["branch"] or candidate_info["branch"] not in ["G", "H", "S", "O"]:
        messages.append("Branch: Please select a valid option")

    # District
    if candidate_info["branch"] == "G":
        candidate_info["district"] = 0
        candidate_info["district_name"] = "0"
    elif candidate_info["branch"] == "S" or candidate_info["branch"] == "H":
        try:
            if int(candidate_info["district"]) <= 0:
                messages.append("District: Please enter a positive integer")
            else:
                candidate_info["district_name"] = str(candidate_info["district"])
        except ValueError:
            messages.append("District: Please enter a positive integer")
    elif candidate_info["branch"] == "O":
        if candidate_info["district_name"] not in statewide_districts:
            messages.append("District: Please select a valid option")
        else:
            candidate_info["district"] = 0

    # First name
    if not candidate_info["first_name"]:
        messages.append("First Name: Field is required")

    # Last name
    if not candidate_info["last_name"]:
        messages.append("Last Name: Field is required")

    # Setup the alternate_names field
    if candidate_info["first_name"] is not None and candidate_info["last_name"] is not None:
        fname = candidate_info["first_name"].strip()
        lname = candidate_info["last_name"].strip()
        alt_names = "{0} {1};{1} {0};{1}".format(fname, lname).split(";")
        ci_alt_names = [] if not candidate_info["alternate_names"] else (candidate_info["alternate_names"].strip(";").split(";"))

        # Combine the names and filter out the duplicates
        ci_alt_names += alt_names
        candidate_info["alternate_names"] = ";".join(list(set(ci_alt_names)))

    # Party
    if not candidate_info["party"] or candidate_info["party"] not in ["R", "D"]:
        messages.append("Party: Field is required")

    # Candidate ID
    if candidate_info["candidate_id"] and not isValidCandidate(candidate_info["candidate_id"]):
        messages.append("Candidate ID: Unknown ID, please check that the value was entered correctly")

    # Committee ID
    if candidate_info["committee_id"] and not isValidCommittee(candidate_info["committee_id"]):
        messages.append("Committee ID: Unknown ID, please check that the value was entered correctly")

    if messages:
        return id, messages

    if not id or id == "None":
        candidate_update_sql = '''
            INSERT INTO contested_races
            (branch, district, district_name, first_name, last_name, incumbent, committee_id, committee_name, party, alternate_names)
            VALUES (:branch, :district, :district_name, :first_name, :last_name, :incumbent, :committee_id, :committee_name, :party, :alternate_names)
            RETURNING id
        '''

        id_result = g.engine.execute(sa.text(candidate_update_sql),
                         branch=candidate_info["branch"],
                         district=candidate_info["district"],
                         district_name=candidate_info["district_name"],
                         first_name=candidate_info["first_name"],
                         last_name=candidate_info["last_name"],
                         incumbent=candidate_info["incumbent"],
                         committee_id=candidate_info["committee_id"],
                         committee_name=candidate_info["committee_name"],
                         party=candidate_info["party"],
                         alternate_names=candidate_info["alternate_names"])
        id_result = id_result.fetchone()
        id = None if not id_result else id_result[0]
    else:
        candidate_update_sql = '''
            UPDATE contested_races
            SET (branch, district, district_name, first_name, last_name, incumbent, committee_id, committee_name, party, alternate_names)
            = (:branch, :district, :district_name, :first_name, :last_name, :incumbent, :committee_id, :committee_name, :party, :alternate_names)
            WHERE id = :id
        '''

        g.engine.execute(sa.text(candidate_update_sql),
                         id=id,
                         branch=candidate_info["branch"],
                         district=candidate_info["district"],
                         district_name=candidate_info["district_name"],
                         first_name=candidate_info["first_name"],
                         last_name=candidate_info["last_name"],
                         incumbent=candidate_info["incumbent"],
                         committee_id=candidate_info["committee_id"],
                         committee_name=candidate_info["committee_name"],
                         party=candidate_info["party"],
                         alternate_names=candidate_info["alternate_names"])

    sslib.updateContestedRaceFunds(g.engine, id)

    return id, []


def insertFederalCandidate(id, candidate_info):

    messages = []

    # Branch
    if not candidate_info["branch"] or candidate_info["branch"] not in ["P", "H", "S"]:
        messages.append("Branch: Please select a valid option")

    # District
    if candidate_info["branch"] == "P" or candidate_info["branch"] == "S":
        candidate_info["district"] = 0
    elif candidate_info["branch"] == "H":
        try:
            if int(candidate_info["district"]) <= 0:
                messages.append("District: Please enter a positive integer")
        except ValueError:
            messages.append("District: Please enter a positive integer")

    # Name
    if not candidate_info["name"]:
        messages.append("Name: Field is required")

    # Party
    if not candidate_info["party"] or candidate_info["party"] not in ["R", "D"]:
        messages.append("Party: Field is required")

    if messages:
        return id, messages

    if not id or id == "None":
        candidate_update_sql = '''
            INSERT INTO federal_races
            (branch, district, name, incumbent, party, fec_link, on_ballot)
            VALUES (:branch, :district, :name, :incumbent, :party, :fec_link, :on_ballot)
            RETURNING id
        '''

        id_result = g.engine.execute(sa.text(candidate_update_sql),
                         branch=candidate_info["branch"],
                         district=candidate_info["district"],
                         name=candidate_info["name"],
                         incumbent=candidate_info["incumbent"],
                         party=candidate_info["party"],
                         fec_link=candidate_info["fec_link"],
                         on_ballot=candidate_info["on_ballot"])
        id_result = id_result.fetchone()
        id = None if not id_result else id_result[0]
    else:
        candidate_update_sql = '''
            UPDATE federal_races
            SET (branch, district, name, incumbent, party, fec_link, on_ballot)
            = (:branch, :district, :name, :incumbent, :party, :fec_link, :on_ballot)
            WHERE id = :id
        '''

        g.engine.execute(sa.text(candidate_update_sql),
                         id=id,
                         branch=candidate_info["branch"],
                         district=candidate_info["district"],
                         name=candidate_info["name"],
                         incumbent=candidate_info["incumbent"],
                         party=candidate_info["party"],
                         fec_link=candidate_info["fec_link"],
                         on_ballot=candidate_info["on_ballot"])

    return id, []


def getHouseSenateContestedRacesCount():
    contested_races_sql = '''
        SELECT
            cr.branch, cr.district, count(*) as total_candidates, sum(cr.total_money) as total_race_money,
            string_agg(concat(cr.first_name, ' ', cr.last_name, CASE WHEN cr.incumbent = 'Y' THEN ' (i)' ELSE '' END), ', ' ORDER BY cr.incumbent DESC, cr.last_name, cr.first_name) as candidates
        FROM contested_races cr
        WHERE branch IN ('H', 'S')
        GROUP BY cr.branch, cr.district
        HAVING count(*) > 1
        ORDER BY total_race_money desc, cr.branch, cr.district
        LIMIT 10
    '''

    candidates_list = g.engine.execute(sa.text(contested_races_sql))

    contested_count = []
    total_candidates = 0
    total_race_money = 0
    previous_district = None
    branch = None

    for c in candidates_list:
        branch_label = "House" if c.branch == "H" else "Senate"
        type_arg = "house_of_representatives" if c.branch == "H" else "senate"
        contested_count.append([c.branch, c.district, c.total_candidates, c.total_race_money, branch_label, type_arg, c.candidates])

    return contested_count
