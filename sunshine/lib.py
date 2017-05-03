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

#============================================================================
def getCommitteeFundsData(committee_id, pre_primary_start, primary_start, post_primary_start):
    current_date = datetime.now().strftime("%Y-%m-%d")
    table_display_data = []
    total_funds_raised = 0.0
    primary_funds_raised = None

    pre_primary_quarterlies = db_session.query(D2Report)\
                                        .join(FiledDoc, D2Report.filed_doc_id==FiledDoc.id)\
                                        .filter(D2Report.committee_id==committee_id)\
                                        .filter(FiledDoc.doc_name=="Quarterly")\
                                        .filter(FiledDoc.reporting_period_begin >= pre_primary_start)\
                                        .filter(FiledDoc.reporting_period_end <= primary_start)\
                                        .order_by(FiledDoc.reporting_period_begin)\
                                        .all()


    primary_quarterlies = db_session.query(D2Report)\
                                    .join(FiledDoc, D2Report.filed_doc_id==FiledDoc.id)\
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
