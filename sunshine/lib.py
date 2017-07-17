from flask import Blueprint, render_template, abort, request, redirect, \
    session as flask_session, g
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

#============================================================================
def getPrimaryDetails(branch):
    primary_details = {}

    if (branch == "G"):
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
    is_comptroller = (type_arg == "comptroller")
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
def getContestedRacesData(type_arg):
    is_house = (type_arg == "house_of_representatives")
    is_senate = (type_arg == "senate")
    is_comptroller = (type_arg == "comptroller")
    is_gubernatorial = (type_arg == "gubernatorial")

    if is_senate:
        contested_races_type = "Senate"
        contested_races_title = "Illinois Senate Contested Races"
    elif is_comptroller:
        contested_races_type = "State Comptroller"
        contested_races_title = "Illinois State Comptroller Contested Race"
    elif is_gubernatorial:
        contested_races_type = "Gubernatorial"
        contested_races_title = "Race for Illinois Governor"

    contested_races = getContestedRacesCsvData(type_arg)

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

    return [contested_races_type, contested_races_title, cand_span, contested_dict]

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
        SELECT cr.*, c.name as committee_committee_name
        FROM contested_races cr
        LEFT JOIN committees c ON (c.id = cr.committee_id)
        WHERE cr.district = :district
          AND cr.branch = :branch
    '''

    races = list(g.engine.execute(sa.text(contested_races_sql),district=district,branch=branch))

    for race in races:
        committee_funds_data = getCommitteeFundsData(race.committee_id, pre_primary_start, primary_start, post_primary_start)
        total_funds = (committee_funds_data[-1][1] if committee_funds_data else 0.0)

        display = (race.first_name or "") + " " + (race.last_name or "") + " (" + (race.party or "") + ")"
        if not race.incumbent:
            pass
        elif (race.incumbent == "Y"):
            display += " *Incumbent"
        elif (race.incumbent != "N"):
            display += " " + race.incumbent

        output.append([display, total_funds])

    if not output:
        return []

    return sorted(output, key=lambda x: -x[1])

#============================================================================
def getCommitteeFundsData(committee_id, pre_primary_start, primary_start, post_primary_start):
    current_date = datetime.now().strftime("%Y-%m-%d")
    table_display_data = []
    total_funds_raised = 0.0
    primary_funds_raised = None

    pre_primary_quarterlies = db_session.query(D2Report)\
                                        .join(FiledDoc, D2Report.filed_doc_id==FiledDoc.id)\
                                        .filter(D2Report.archived == False)\
                                        .filter(FiledDoc.archived == False)\
                                        .filter(D2Report.committee_id==committee_id)\
                                        .filter(FiledDoc.doc_name=="Quarterly")\
                                        .filter(FiledDoc.reporting_period_begin >= pre_primary_start)\
                                        .filter(FiledDoc.reporting_period_end <= primary_start)\
                                        .order_by(FiledDoc.reporting_period_begin)\
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
                                    .all()

    if current_date >= post_primary_start:
        if primary_quarterlies:
            # Add rows for each primary quarterly report.
            for i, rpt in enumerate(primary_quarterlies):
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
                                            .order_by(FiledDoc.reporting_period_end.desc())\
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
