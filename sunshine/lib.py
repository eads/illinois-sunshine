from flask import Blueprint, render_template, abort, request, redirect, \
    session as flask_session, g, current_app
from sunshine.database import db_session
from sunshine.models import Receipt, FiledDoc, D2Report
from sunshine.cache import cache, make_cache_key, CACHE_TIMEOUT
from sunshine.app_config import FLUSH_KEY
import sqlalchemy as sa
from sqlalchemy.sql import func
from datetime import datetime, timedelta
from dateutil.parser import parse
import collections
import csv
import os
import psycopg2
import traceback

#============================================================================
def getPrimaryDetails(branch):
    primary_details = {}

    # Check to see if these dates are the same
    primary_details["pre_primary_start"] = "2017-01-01"
    primary_details["primary_start"] = "2018-01-01"
    primary_details["primary_end"] = "2018-03-20"
    primary_details["primary_quarterly_end"] = "2018-03-31"

    if not primary_details:
        return {}

    if "primary_end" in primary_details:
        primary_details["post_primary_start"] = (parse(primary_details["primary_end"]) + timedelta(days=1)).strftime("%Y-%m-%d")
        primary_details["is_after_primary"] = parse(primary_details["primary_end"]).date() < datetime.today().date()

    return primary_details

#============================================================================
def getContestedRacesCsvData(type_arg):
    is_comptroller = (type_arg == "statewide_office")
    is_gubernatorial = (type_arg == "gubernatorial")
    input_filename = "contested_races.csv"

    if is_comptroller:
        input_filename = "comptroller_contested_race.csv"
    elif is_gubernatorial:
        input_filename = "gubernatorial_contested_races.csv"

    return list(csv.DictReader(open(
        os.getcwd() + '/sunshine/' + input_filename
    )))

#============================================================================
def getCandidateDataFromCsvRow(row):
    first_names = row['First'].split(';')
    last_names = row['Last'].split(';')

    first_name = first_names[0].strip()
    last_name = last_names[0].strip()

    return {'last': last_name, 'first': first_name,'incumbent': row['Incumbent'],'party': row['Party']}

#============================================================================
def getAllCandidateFunds(district, branch):
    output = []

    primary_details = getPrimaryDetails(branch)
    pre_primary_start = primary_details["pre_primary_start"]
    primary_start = primary_details["primary_start"]
    primary_end = primary_details["primary_end"]
    primary_quarterly_end = primary_details["primary_quarterly_end"]
    post_primary_start = primary_details["post_primary_start"]
    is_after_primary = primary_details["is_after_primary"]

    contested_races_sql = '''
        SELECT committee_id, last_name, first_name, party, incumbent, sum(total_funds) as total_funds, sum(funds_available) as funds_available
        FROM contested_races
        WHERE district_name = :district AND branch = :branch
        GROUP BY committee_id, last_name, first_name, party, incumbent
    '''

    races = list(g.engine.execute(sa.text(contested_races_sql),district=str(district),branch=branch))
    committees_cash_on_hand = getCommitteesCashOnHand([race.committee_id for race in races])

    for race in races:
        funds_available = race.funds_available
        total_funds_raised = race.total_funds

        cash_on_hand = committees_cash_on_hand.get(race.committee_id, 0)

        display = (race.first_name or "") + " " + (race.last_name or "") + " (" + (race.party or "") + ")"
        if not race.incumbent:
            pass
        elif (race.incumbent == "Y"):
            display += " *Incumbent"
        elif (race.incumbent != "N"):
            display += " " + race.incumbent

        output.append([display, cash_on_hand, total_funds_raised, funds_available])

    if not output:
        return []

    return sorted(output, key=lambda x: -x[2])

#============================================================================
def getCommitteeCashOnHand(committee_id):
    latest_filing, recent_total, controlled_amount, params = getCommitteeRecentFilingData(committee_id)
    return controlled_amount

#============================================================================
def getCommitteesCashOnHand(committee_ids):
    committees_data = getCommitteesRecentFilingData(committee_ids)
    return dict([(k, v[2]) for k, v in committees_data.items()])

#============================================================================
def getCommitteesRecentFilingData(committee_ids=[]):
    if not committee_ids:
        return {}

    latest_filings_sql = '''
        SELECT mrf.*
        FROM most_recent_filings mrf
        JOIN (
            SELECT committee_id, max(received_datetime) as received_datetime
            FROM most_recent_filings
            GROUP BY committee_id
        ) mrf_sub ON (mrf_sub.received_datetime = mrf.received_datetime and mrf_sub.committee_id = mrf.committee_id)
        WHERE mrf.committee_id IN :committee_ids
    '''

    latest_filings = list(g.engine.execute(sa.text(latest_filings_sql), committee_ids=tuple(committee_ids)))

    if not latest_filings:
        return {}

    processed_committee_ids = {}

    for latest_filing in latest_filings:
        if not latest_filing:
            continue

        params = {'committee_id': latest_filing.committee_id}

        if not latest_filing['reporting_period_end']:
          latest_filing['reporting_period_end'] = datetime.now().date() - timedelta(days=90)

        if latest_filing['end_funds_available'] or latest_filing['end_funds_available'] == 0:
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
        processed_committee_ids[latest_filing.committee_id] = [latest_filing, recent_total, controlled_amount, params]

    return processed_committee_ids

#============================================================================
def getCommitteeRecentFilingData(committee_id):

    if not committee_id:
        return [None, 0, 0, {}]

    latest_filing = '''
        SELECT * FROM most_recent_filings
        WHERE committee_id = :committee_id
        ORDER BY received_datetime DESC
        LIMIT 1
    '''

    latest_filing = g.engine.execute(sa.text(latest_filing), committee_id=committee_id)

    if not latest_filing:
        return [None, 0, 0, {}]

    latest_filing = dict(latest_filing.first())
    params = {'committee_id': committee_id}

    if not latest_filing['reporting_period_end']:
      latest_filing['reporting_period_end'] = datetime.now().date() - timedelta(days=90)

    if latest_filing['end_funds_available'] or latest_filing['end_funds_available'] == 0:
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

    return [latest_filing, recent_total, controlled_amount, params]

#============================================================================
def getCommitteeFundsData(committee_id, pre_primary_start, primary_start, post_primary_start):
    current_date = datetime.now().strftime("%Y-%m-%d")
    table_display_data = []
    total_funds_raised = 0.0
    primary_funds_raised = None
    # Date used to eliminate duplicates
    temp_dtstart = ""

    pre_primary_quarterlies = db_session.query(D2Report)\
                                        .join(FiledDoc, D2Report.filed_doc_id==FiledDoc.id)\
                                        .filter(D2Report.archived == False)\
                                        .filter(FiledDoc.archived == False)\
                                        .filter(D2Report.committee_id==committee_id)\
                                        .filter(FiledDoc.doc_name=="Quarterly")\
                                        .filter(FiledDoc.reporting_period_begin >= pre_primary_start)\
                                        .filter(FiledDoc.reporting_period_end <= primary_start)\
                                        .order_by(FiledDoc.reporting_period_begin)\
                                        .order_by(FiledDoc.reporting_period_end)\
                                        .order_by(FiledDoc.received_datetime.desc())\
                                        .all()


    primary_quarterlies = db_session.query(D2Report)\
                                    .join(FiledDoc, D2Report.filed_doc_id==FiledDoc.id)\
                                    .filter(D2Report.archived == False)\
                                    .filter(FiledDoc.archived == False)\
                                    .filter(D2Report.committee_id==committee_id)\
                                    .filter(FiledDoc.doc_name=="Quarterly")\
                                    .filter(FiledDoc.reporting_period_begin >= primary_start)\
                                    .filter(FiledDoc.reporting_period_end <= current_date)\
                                    .order_by(FiledDoc.reporting_period_begin)\
                                    .order_by(FiledDoc.reporting_period_end)\
                                    .order_by(FiledDoc.received_datetime.desc())\
                                    .all()

    if current_date >= post_primary_start:
        if primary_quarterlies:
            # Add rows for each primary quarterly report.
            for i, rpt in enumerate(primary_quarterlies):

                # If the reporting period begin date equals the temporary start date, skip the record
                if temp_dtstart == rpt.filed_doc.reporting_period_begin:
                    continue

                temp_dtstart = rpt.filed_doc.reporting_period_begin

                rpt_label = "Funds Raised {dtstart:%b} {dtstart.day}, {dtstart.year} to {dtend:%b} {dtend.day}, {dtend.year}".format(
                    dtstart = rpt.filed_doc.reporting_period_begin,
                    dtend = rpt.filed_doc.reporting_period_end
                )
                rpt_total = rpt.total_receipts

                # For the first quarterly (the primary quarterly) use the end_funds_available.
                if i == 0:
                    rpt_label = "Funds Available After Primary"
                    rpt_total = rpt.end_funds_available

                table_display_data.append([rpt_label, rpt_total])
                last_quarterly_date = rpt.filed_doc.reporting_period_end
                total_funds_raised += rpt_total

            # Add a row for funds raised since the last primary quarterly report (pulled from Receipt).
            total_receipts = getReceiptsTotal(committee_id, "A-1", last_quarterly_date, current_date)
            receipts_label = "Funds Raised Since {dt:%b} {dt.day}, {dt.year}".format(dt = last_quarterly_date)
            table_display_data.append([receipts_label, total_receipts])
            total_funds_raised += total_receipts

            # Add a row for the total funds raised.
            table_display_data.append(["Total Funds Raised", total_funds_raised])
        else:
            if pre_primary_quarterlies:
                # Add the funds available from the last pre-primary quarterly report.
                pre_primary_end_date = "{dt:%b} {dt.day}, {dt.year}".format(dt = pre_primary_quarterlies[-1].filed_doc.reporting_period_end)
                table_display_data.append(["Funds Available on " + pre_primary_end_date + " Quarterly Report", pre_primary_quarterlies[-1].end_funds_available])
                total_funds_raised = pre_primary_quarterlies[-1].end_funds_available
                last_quarterly_date = pre_primary_quarterlies[-1].filed_doc.reporting_period_end
            else:
                total_funds_raised = 0.0
                last_quarterly_date = parse(pre_primary_start)

            # Add contributions since last quaterly report.
            total_receipts = getReceiptsTotal(committee_id, "A-1", last_quarterly_date, current_date)
            receipts_label = "Contributions Since {dt:%b} {dt.day}, {dt.year}".format(dt = last_quarterly_date)
            table_display_data.append([receipts_label, total_receipts])
            total_funds_raised += total_receipts

            # Add a row for the total funds.
            table_display_data.append(["Total Funds", total_funds_raised])

    else:
        # Default the last quarterly date to the day before the pre-primary starts.
        last_quarterly_date = parse(pre_primary_start) - timedelta(days=1)

        pre_pre_primary_quarterly = db_session.query(D2Report)\
                                            .join(FiledDoc, D2Report.filed_doc_id==FiledDoc.id)\
                                            .filter(D2Report.archived == False)\
                                            .filter(FiledDoc.archived == False)\
                                            .filter(D2Report.committee_id==committee_id)\
                                            .filter(FiledDoc.doc_name=="Quarterly")\
                                            .filter(FiledDoc.reporting_period_end <= last_quarterly_date)\
                                            .order_by(FiledDoc.reporting_period_begin.desc())\
                                            .order_by(FiledDoc.reporting_period_end)\
                                            .order_by(FiledDoc.received_datetime.desc())\
                                            .first()

        # Add the funds available from the last quarterly report from before the pre-primary start date.
        pre_pre_primary_end_date = "{dt:%b} {dt.day}, {dt.year}".format(dt = last_quarterly_date)
        pre_pre_primary_funds = 0 if not pre_pre_primary_quarterly else pre_pre_primary_quarterly.end_funds_available
        table_display_data.append(["Funds Available on " + pre_pre_primary_end_date + " Quarterly Report", pre_pre_primary_funds])
        total_funds_raised += pre_pre_primary_funds

        # Add rows for each pre-primary quarterly report.
        for rpt in pre_primary_quarterlies:
            rpt_label = "Funds Raised {dtstart:%b} {dtstart.day}, {dtstart.year} to {dtend:%b} {dtend.day}, {dtend.year}".format(
                dtstart = rpt.filed_doc.reporting_period_begin,
                dtend = rpt.filed_doc.reporting_period_end
            )

            # If the reporting period being date equals the temporary start date, skip the record
            if temp_dtstart != rpt.filed_doc.reporting_period_begin:
                temp_dtstart = rpt.filed_doc.reporting_period_begin

                table_display_data.append([rpt_label, rpt.total_receipts])
                last_quarterly_date = rpt.filed_doc.reporting_period_end
                total_funds_raised += rpt.total_receipts


        # Add funds raised since last quaterly report.
        total_receipts = getReceiptsTotal(committee_id, "A-1", last_quarterly_date, current_date)
        receipts_label = "Funds Raised Since {dt:%b} {dt.day}, {dt.year}".format(dt = last_quarterly_date)
        table_display_data.append([receipts_label, total_receipts])
        total_funds_raised += total_receipts

        # Add a row for the total funds raise.
        table_display_data.append(["Total Funds Raised", total_funds_raised])

    return table_display_data

#============================================================================
def getFundsRaisedTotal(committee_id, quarterly_start_date, next_quarterly_start_date, receipt_end_date):
    pre_primary_total_raised = db_session.query(func.sum(D2Report.total_receipts))\
        .join(FiledDoc, D2Report.filed_doc_id==FiledDoc.id)\
        .filter(D2Report.committee_id==committee_id)\
        .filter(FiledDoc.doc_name=="Quarterly")\
        .filter(FiledDoc.reporting_period_begin >= quarterly_start_date)\
        .filter(FiledDoc.reporting_period_end < next_quarterly_start_date)\
        .scalar()

    # If the Primary Quarterly report hasn't been filed yet, then return None.
    if pre_primary_total_raised is None:
        return None

    # Add contributions since last quarterly report.
    quarterly_end_date = parse(next_quarterly_start_date) - timedelta(days=1)
    contributions = getReceiptsTotal(committee_id, "A-1", quarterly_end_date, receipt_end_date)

    return pre_primary_total_raised + contributions

#============================================================================
def getReceiptsTotal(committee_id, doc_name, last_period_end, last_receipt_date=None):
    total_receipts = db_session.query(func.sum(Receipt.amount))\
                            .join(FiledDoc, Receipt.filed_doc_id==FiledDoc.id)\
                            .filter(Receipt.committee_id==committee_id)\
                            .filter(FiledDoc.doc_name==doc_name)\
                            .filter(FiledDoc.reporting_period_begin >= last_period_end)

    if last_receipt_date:
        total_receipts = total_receipts.filter(FiledDoc.received_datetime <= last_receipt_date)

    total_receipts = total_receipts.scalar()
    return total_receipts if total_receipts is not None else 0.0

#============================================================================
def getContestedRacesInformation(type_arg):
    is_house = (type_arg == "house_of_representatives")
    is_senate = (type_arg == "senate")
    is_statewide_office = (type_arg == "statewide_office")

    if is_senate:
        contested_races_type = "Senate"
        contested_races_title = "Illinois Senate Races"
        branch = "S"
    elif is_statewide_office:
        contested_races_type = "Statewide Offices"
        contested_races_title = "Illinois Constitutional Officer Races"
        branch = "O"
    else:
        contested_races_type = "House of Representatives"
        contested_races_title = "Illinois House of Representatives Races"
        branch = "H"

    order_by = "cr.district_name" if branch == "O" else "cr.district"

    contested_races_sql = '''
        SELECT cr.*, concat_ws(' ', cr.first_name, cr.last_name, CASE WHEN cr.incumbent = 'Y' THEN ' (i)' ELSE NULL END) as pretty_name
        FROM contested_races cr
        WHERE cr.branch = :branch
        ORDER BY {0}
    '''.format(order_by)

    contest_race_list = list(g.engine.execute(sa.text(contested_races_sql), branch=branch))

    total_candidates = 0
    total_race_money = 0
    previous_district = None
    contested_races_output = {}

    for c in contest_race_list:
        key = c.branch + "__" + str(c.district_name)
        if not contested_races_output.get(key):
            contested_races_output[key] = {
                "branch": c.branch,
                "district": c.district,
                "district_name": c.district_name,
                "total_candidates": 0,
                "candidate_names": "",
                "total_race_money": 0,
                "district_id": "_".join(str(c.district_name).split()),
                "district_label": ("Senate" if c.branch == "S" else "House") + " District",
                "district_sort": c.district if c.branch in ["H", "S"] else c.district_name,
                "R": [],
                "D": []
            }

        contested_races_output[key]["total_candidates"] += 1
        contested_races_output[key]["total_race_money"] += c.total_money
        contested_races_output[key].get(c.party, []).append(c)

    # Convert from dict to list.
    contested_races_output = list(contested_races_output.values())

    # Sort list.
    contested_races_output = sorted(contested_races_output, key=lambda k: (k["branch"], k["district_sort"]))

    # Sort the Rep and Dem lists and make them equal lengths.
    for race in contested_races_output:
        sorted_cands = sorted((race["D"] + race["R"]), key = lambda k: (k["incumbent"] != "Y", k["last_name"], k["first_name"]))
        race["candidate_names"] = "; ".join([cand["pretty_name"] for cand in sorted_cands])
        race["D"] = sorted(race["D"], key=lambda k: k["total_funds"], reverse=True)
        race["R"] = sorted(race["R"], key=lambda k: k["total_funds"], reverse=True)

        while len(race["D"]) < len(race["R"]):
            race["D"].append({})

        while len(race["R"]) < len(race["D"]):
            race["R"].append({})

    return [contested_races_type, contested_races_title, contested_races_output]

#============================================================================
def getContestedRace(id):
    contested_races_sql = '''SELECT * FROM contested_races WHERE id = :id'''
    return g.engine.execute(sa.text(contested_races_sql), id=id).fetchone()

#============================================================================
def deleteContestedRace(id):
    contested_races_sql = '''DELETE FROM contested_races WHERE id = :id'''
    try:
        g.engine.execute(sa.text(contested_races_sql), id=id)
    except (sa.exc.ProgrammingError, psycopg2.ProgrammingError):
        return False
    return True

#============================================================================
def updateContestedRacesFunds(connection, races = []):

    # Get candidate funds data
    try:
        if not races:
            races_sql = '''SELECT * FROM contested_races'''
            races = list(connection.execute(sa.text(races_sql)))
    except (psycopg2.ProgrammingError, sa.exc.ProgrammingError):
        current_app.logger.error('Problem retrieving contested_race funds data: ')
        current_app.logger.error(traceback.print_exc())

    races_data = []
    for race in races:
        updateContestedRaceFunds(connection, race.id, race)


#============================================================================
def updateContestedRaceFunds(connection, id, race = None):
    race_data = None

    try:
        if not race:
            race_sql = '''SELECT * FROM contested_races WHERE id = :id'''
            race = connection.execute(sa.text(race_sql), id=id).fetchone()
    except (psycopg2.ProgrammingError, sa.exc.ProgrammingError):
        current_app.logger.error('Problem retrieving contested_races funds data: ')
        current_app.logger.error(traceback.print_exc())

    if not race:
        return

    try:
        primary_details = getPrimaryDetails(race.branch)
        pre_primary_start = primary_details.get("pre_primary_start")
        primary_start = primary_details.get("primary_start")
        primary_end = primary_details.get("primary_end")
        primary_quarterly_end = primary_details.get("primary_quarterly_end")
        post_primary_start = primary_details.get("post_primary_start")
        is_after_primary = primary_details.get("is_after_primary")

        # If there are no dates for this race, then go to the next race.
        if not pre_primary_start:
            return

        supporting_funds = 0
        opposing_funds = 0
        controlled_amount = 0
        funds_available = 0
        contributions = 0
        total_funds = 0
        investments = 0
        debts = 0
        total_money = 0

        # Get supporting/opposing funds
        cand_names = race.alternate_names.split(";")
        for name in cand_names:
            supp_funds, opp_funds = get_candidate_funds_byname(connection, name)
            supporting_funds += supp_funds
            opposing_funds += opp_funds

        # Get candidate contested race funds
        if race.committee_id:
            committee_funds_data = getCommitteeFundsData(race.committee_id, pre_primary_start, primary_start, post_primary_start)
            funds_available = getFundsRaisedTotal(race.committee_id, pre_primary_start, primary_start, primary_end) if is_after_primary else 0
            total_funds = (committee_funds_data[-1][1] if committee_funds_data else 0.0)

            committee, recent_receipts, recent_total, latest_filing, controlled_amount, ending_funds, investments, debts, expenditures, total_expenditures = get_committee_details(connection, race.committee_id)
            contributions = recent_total
            investments = latest_filing['total_investments'] if latest_filing else 0
            debts = latest_filing['total_debts'] if latest_filing else 0

        total_money = supporting_funds + opposing_funds + total_funds

        race_data = {
            "id": race.id,
            "total_money": total_money,
            "total_funds": total_funds,
            "funds_available": funds_available,
            "contributions": contributions,
            "investments": investments,
            "debts": debts,
            "supporting_funds": supporting_funds,
            "opposing_funds": opposing_funds
        }
    except (psycopg2.ProgrammingError, sa.exc.ProgrammingError):
        current_app.logger.error('Problem calculating contested_race funds data: ')
        current_app.logger.error(traceback.print_exc())

    if not race_data:
        return

    # Update candidate funds data
    try:
        connection.execute(sa.text('''
            UPDATE contested_races SET
                total_money = :total_money,
                total_funds = :total_funds,
                funds_available = :funds_available,
                contributions = :contributions,
                investments = :investments,
                debts = :debts,
                supporting_funds = :supporting_funds,
                opposing_funds = :opposing_funds
            WHERE id = :id
        '''), **race_data)
    except (psycopg2.ProgrammingError, sa.exc.ProgrammingError):
        current_app.logger.error('Problem updating contested_races funds data: ')
        current_app.logger.error(traceback.print_exc())

#============================================================================
def get_candidate_funds_byname(connection, candidate_name):

    d2_part = '9B'
    expended_date = datetime(2017, 1, 1, 0, 0)

    supporting_funds_sql = '''(
        SELECT
          COALESCE(SUM(e.amount), 0) AS amount
        FROM condensed_expenditures AS e
        WHERE e.candidate_name = :candidate_name
          AND e.d2_part = :d2_part
          AND e.expended_date > :expended_date
          AND e.supporting = 'true'
        )
    '''

    try:
        supporting_funds = connection.execute(
            sa.text(supporting_funds_sql),
            candidate_name=candidate_name,
            d2_part=d2_part,
            expended_date=expended_date
        ).fetchone().amount
    except (sa.exc.ProgrammingError, psycopg2.ProgrammingError) as e:
        raise e

    opposing_funds_sql = '''(
        SELECT
          COALESCE(SUM(e.amount), 0) AS amount
        FROM condensed_expenditures AS e
        WHERE e.candidate_name = :candidate_name
          AND e.d2_part = :d2_part
          AND e.expended_date > :expended_date
          AND e.opposing = 'true'
        )
    '''

    try:
        opposing_funds = connection.execute(
            sa.text(opposing_funds_sql),
            candidate_name=candidate_name,
            d2_part=d2_part,
            expended_date=expended_date
        ).fetchone().amount
    except (sa.exc.ProgrammingError, psycopg2.ProgrammingError) as e:
        raise e

    return supporting_funds, opposing_funds

#============================================================================
def get_committee_details(connection, committee_id):
    default_return = [None, None, 0, None, 0, 0, None, None, None, 0]

    default_return = [None, None, 0, None, 0, 0, 0, 0, 0, 0]

    try:
        committee_id = int(committee_id)
    except ValueError:
        return default_return

    comm_sql = '''(
        SELECT *
        FROM committees
        WHERE id = :committee_id
        )
    '''
    committee = executeTransaction(
        connection,
        sa.text(comm_sql),
        committee_id=committee_id
    ).fetchone()

    if not committee:
        return default_return

    latest_filing = '''(
        SELECT * FROM most_recent_filings
        WHERE committee_id = :committee_id
        ORDER BY received_datetime DESC
        LIMIT 1
        )
    '''

    latest_filing = dict(executeTransaction(
        connection,
        sa.text(latest_filing),
        committee_id=committee_id
    ).fetchone())

    params = {'committee_id': committee_id}

    if not latest_filing['reporting_period_end']:
        latest_filing['reporting_period_end'] = \
            datetime.now().date() - timedelta(days=90)

    if (
        latest_filing['end_funds_available'] or
        latest_filing['end_funds_available'] == 0
    ):

        recent_receipts = '''(
            SELECT
              COALESCE(SUM(receipts.amount), 0) AS amount
            FROM condensed_receipts AS receipts
            JOIN filed_docs AS filed
              ON receipts.filed_doc_id = filed.id
            WHERE receipts.committee_id = :committee_id
              AND receipts.received_date > :end_date
            )
        '''
        controlled_amount = latest_filing['end_funds_available'] if latest_filing else 0

        params['end_date'] = latest_filing['reporting_period_end'] if latest_filing else None
        end_date = latest_filing['reporting_period_end'] if latest_filing else None

    else:

        recent_receipts = '''(
            SELECT
              COALESCE(SUM(receipts.amount), 0) AS amount
            FROM condensed_receipts AS receipts
            JOIN filed_docs AS filed
              ON receipts.filed_doc_id = filed.id
            WHERE receipts.committee_id = :committee_id
            )
        '''

        controlled_amount = 0

    recent_total = executeTransaction(
        connection,
        sa.text(recent_receipts),
        **params
    ).fetchone().amount
    controlled_amount += recent_total

    quarterlies = '''(
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
        )
    '''

    quarterlies = executeTransaction(
        connection,
        sa.text(quarterlies),
        committee_id=committee_id
    )

    ending_funds = [
        [
            r.end_funds_available,
            r.reporting_period_end.year,
            r.reporting_period_end.month,
            r.reporting_period_end.day
        ] for r in quarterlies
    ]

    investments = [
        [
            r.total_investments,
            r.reporting_period_end.year,
            r.reporting_period_end.month,
            r.reporting_period_end.day
        ] for r in quarterlies
    ]

    debts = [
        [
            (r.debts_itemized + r.debts_non_itemized),
            r.reporting_period_end.year,
            r.reporting_period_end.month,
            r.reporting_period_end.day
        ] for r in quarterlies
    ]

    expenditures = [
        [
            r.total_expenditures,
            r.reporting_period_end.year,
            r.reporting_period_end.month,
            r.reporting_period_end.day
        ] for r in quarterlies
    ]

    # accomodate for independent expenditures past last filing date

    total_expenditures = sum([r.total_expenditures for r in quarterlies])

    return committee, recent_receipts, recent_total, latest_filing, controlled_amount, ending_funds, investments, debts, expenditures, total_expenditures

#============================================================================
def executeTransaction(connection, query, **kwargs):
    return connection.execute(query, **kwargs)
