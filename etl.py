from sunshine.models import Committee, Candidate, Officer, Candidacy, \
    D2Report, FiledDoc, Receipt, Expenditure, Investment
import os
from datetime import datetime, timedelta
from dateutil.parser import parse
import sqlalchemy as sa
import csv
from csvkit.cleanup import RowChecker
from collections import OrderedDict
from typeinferer import TypeInferer
import psycopg2
import traceback
from psycopg2.extensions import AsIs
from sunshine import lib as sslib

import logging
logger = logging.getLogger(__name__)

try:
    from raven.conf import setup_logging
    from raven.handlers.logging import SentryHandler
    from sunshine.app_config import SENTRY_DSN

    if SENTRY_DSN:
        handler = SentryHandler(SENTRY_DSN)
        setup_logging(handler)
except ImportError:
    pass
except KeyError:
    pass


class SunshineTransformLoad(object):
    update_ignore_fields = []
    update_conversions = {}

    def __init__(self,
                 connection,
                 metadata=None,
                 chunk_size=50000,
                 file_path='downloads'):

        self.connection = connection

        self.chunk_size = chunk_size

        if metadata:
            self.metadata = metadata
            self.initializeDB()

        self.file_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            file_path,
            self.filename)

    def executeTransaction(self, query, raise_exc=False, *args, **kwargs):
        trans = self.connection.begin()

        try:
            if kwargs:
                self.connection.execute(query, **kwargs)
            else:
                self.connection.execute(query, *args)
            trans.commit()
        except sa.exc.ProgrammingError as e:
            logger.error(e, exc_info=True)
            trans.rollback()
            print(e)
            if raise_exc:
                raise e

    def executeOutsideTransaction(self, query):

        self.connection.connection.set_isolation_level(0)
        curs = self.connection.connection.cursor()

        try:
            curs.execute(query)
        except psycopg2.ProgrammingError:
            pass

    def addNameColumn(self):

        sql_table = sa.Table(
            self.table_name,
            sa.MetaData(),
            autoload=True,
            autoload_with=self.connection.engine
        )

        if 'search_name' not in sql_table.columns.keys():

            add_name_col = '''
                ALTER TABLE {0} ADD COLUMN search_name tsvector
            '''.format(self.table_name)

            self.executeTransaction(add_name_col, raise_exc=True)

            add_names = '''
                UPDATE {0} SET
                  search_name = to_tsvector('english', COALESCE(first_name, '') || ' ' ||
                                                       COALESCE(REPLACE(last_name, '&', ''), ''))
            '''.format(self.table_name)

            self.executeTransaction(add_names)

            add_index = '''
                CREATE INDEX {0}_search_name_index ON {0}
                USING gin(search_name)
            '''.format(self.table_name)

            self.executeTransaction(add_index)

            trigger = '''
                CREATE TRIGGER {0}_search_update
                BEFORE INSERT OR UPDATE ON {0}
                FOR EACH ROW EXECUTE PROCEDURE
                tsvector_update_trigger(search_name,
                                        'pg_catalog.english',
                                        first_name,
                                        last_name)
            '''.format(self.table_name)

            self.executeTransaction(trigger)

    def addDateColumn(self, date_col):
        sql_table = sa.Table(
            self.table_name,
            sa.MetaData(),
            autoload=True,
            autoload_with=self.connection.engine
        )

        if 'search_date' not in sql_table.columns.keys():

            add_date_col = '''
                ALTER TABLE {0} ADD COLUMN search_date TIMESTAMP
            '''.format(self.table_name)

            self.executeTransaction(add_date_col)

            add_index = '''
                CREATE INDEX {0}_search_date_index ON {0} (search_date)
            '''.format(self.table_name)

            self.executeTransaction(add_index)

        add_dates = '''
            UPDATE {0} SET
              search_date = subq.search_date
            FROM (
                SELECT
                  {1}::timestamp AS search_date,
                  id
                FROM {0}
            ) AS subq
            WHERE {0}.id = subq.id
              AND {0}.search_date IS NULL
        '''.format(self.table_name, date_col)

        self.executeTransaction(add_dates)

    def initializeDB(self):
        enum = '''
            CREATE TYPE committee_position AS ENUM (
              'support',
              'oppose'
            )
        '''

        try:
            self.executeTransaction(enum)
        except:
            pass

        self.metadata.create_all(bind=self.connection.engine)

    def makeRawTable(self):
        inferer = TypeInferer(self.file_path)
        inferer.infer()

        sql_table = sa.Table('raw_{0}'.format(self.table_name),
                             sa.MetaData())

        for column_name, column_type in inferer.types.items():
            sql_table.append_column(sa.Column(column_name, column_type()))

        dialect = sa.dialects.postgresql.dialect()
        create_table = str(
            sa.schema.CreateTable(sql_table).compile(dialect=dialect)
        ).strip(';')

        self.executeTransaction('DROP TABLE IF EXISTS raw_{0}'.format(
            self.table_name
        ))
        self.executeTransaction(create_table)

    def writeRawToDisk(self):
        with open(self.file_path, 'r', encoding='latin-1') as inp:
            reader = csv.reader(inp, delimiter='\t', quoting=csv.QUOTE_NONE)
            self.raw_header = next(reader)
            checker = RowChecker(reader)

            with open('%s_raw.csv' % self.file_path, 'w') as outp:
                writer = csv.writer(outp)

                writer.writerow(self.raw_header)

                for row in checker.checked_rows():
                    writer.writerow(row)

    def bulkLoadRawData(self):
        import psycopg2
        from sunshine.app_config import DB_USER, DB_PW, DB_HOST, \
            DB_PORT, DB_NAME

        DB_CONN_STR = 'host={0} dbname={1} user={2} port={3} password={4}'\
            .format(DB_HOST, DB_NAME, DB_USER, DB_PORT, DB_PW)

        copy_st = '''
            COPY raw_{0} FROM STDIN WITH CSV HEADER DELIMITER ','
        '''.format(self.table_name)

        with open('%s_raw.csv' % self.file_path, 'r') as f:
            next(f)
            with psycopg2.connect(DB_CONN_STR) as conn:
                with conn.cursor() as curs:
                    try:
                        curs.copy_expert(copy_st, f)
                    except psycopg2.IntegrityError as e:
                        logger.error(e, exc_info=True)
                        print(e)
                        conn.rollback()

        os.remove('%s_raw.csv' % self.file_path)

    def findNewRecords(self):
        create_new_record_table = '''
            CREATE TABLE new_{0} AS (
                SELECT raw."ID"
                FROM raw_{0} AS raw
                LEFT JOIN {0} AS dat
                  ON raw."ID" = dat.id
                WHERE dat.id IS NULL
            )
        '''.format(self.table_name)

        self.executeTransaction('DROP TABLE IF EXISTS new_{0}'.format(
            self.table_name
        ))
        self.executeTransaction(create_new_record_table)

    def iterIncomingData(self):
        incoming = '''
            SELECT raw.*
            FROM raw_{0} AS raw
            JOIN new_{0} AS new
              USING("ID")
        '''.format(self.table_name)

        for record in self.connection.engine.execute(incoming):
            yield record

    def transform(self):
        for row in self.iterIncomingData():
            values = []
            for value in row.values():
                if isinstance(value, str):
                    if value.strip() == '':
                        values.append(None)
                    else:
                        values.append(value)
                else:
                    values.append(value)
            yield OrderedDict(zip(self.header, values))

    @property
    def insert(self):
        return '''
            INSERT INTO {0} ({1}) VALUES ({2})
        '''.format(self.table_name,
                   ','.join(self.header),
                   ','.join([':%s' % h for h in self.header]))

    def load(self, update_existing=False):
        self.makeRawTable()
        self.writeRawToDisk()
        self.bulkLoadRawData()

        self.findNewRecords()
        self.insertNewRecords()

        if update_existing:
            self.updateExistingRecords()

    def updateExistingRecords(self):
        fields = ','.join([
            ('{0}=' + self.update_conversions.get(raw, 's."{1}"')).format(clean, raw)
            for clean, raw in zip(self.header, self.raw_header)
        ])

        update = '''
            UPDATE {table_name} SET
              {fields}
            FROM (
              SELECT * FROM raw_{table_name}
            ) AS s
            WHERE {table_name}.id = s."ID"
        '''.format(table_name=self.table_name,
                   fields=fields)

        self.executeTransaction(update)

    def insertNewRecords(self):
        rows = []
        i = 0
        for row in self.transform():
            rows.append(row)
            if len(rows) % self.chunk_size is 0:

                self.executeTransaction(sa.text(self.insert), *rows)

                print('Inserted %s %s' % (i, self.table_name))
                rows = []

            i += 1

        if rows:
            if len(rows) == 1:
                self.executeTransaction(sa.text(self.insert), **rows[0])
            else:
                self.executeTransaction(sa.text(self.insert), *rows)

        logger.info('inserted %s %s' % (i, self.table_name))


class SunshineCommittees(SunshineTransformLoad):

    table_name = 'committees'
    header = Committee.__table__.columns.keys()
    filename = 'Committees.txt'
    update_ignore_fields = ['search_name', 'search_date']
    update_conversions = {
        "Status": "CASE WHEN s.\"Status\" = 'A' THEN True ELSE False END",
        "CanSuppOpp": "(CASE s.\"CanSuppOpp\" WHEN 'O' THEN 'oppose' WHEN 'S' THEN 'support' ELSE null END)::committee_position",
        "PolicySuppOpp": "(CASE s.\"PolicySuppOpp\" WHEN 'O' THEN 'oppose' WHEN 'S' THEN 'support' ELSE null END)::committee_position",
        "TypeOfCommittee": "CASE s.\"TypeOfCommittee\" WHEN 'Independent Expenditure' THEN 'Super PAC' ELSE s.\"TypeOfCommittee\" END"
    }

    def addNameColumn(self):

        sql_table = sa.Table(
            self.table_name,
            sa.MetaData(),
            autoload=True,
            autoload_with=self.connection.engine
        )

        if 'search_name' not in sql_table.columns.keys():

            add_name_col = '''
                ALTER TABLE {0} ADD COLUMN search_name tsvector
            '''.format(self.table_name)

            try:
                self.executeTransaction(add_name_col, raise_exc=True)
            except sa.exc.ProgrammingError:
                return

            add_names = '''
                UPDATE {0} SET
                  search_name = to_tsvector('english', REPLACE(name, '&', ''))
            '''.format(self.table_name)

            self.executeTransaction(add_names)

            add_index = '''
                CREATE INDEX {0}_search_name_index ON {0}
                USING gin(search_name)
            '''.format(self.table_name)

            self.executeTransaction(add_index)

            trigger = '''
                CREATE TRIGGER {0}_search_update
                BEFORE INSERT OR UPDATE ON {0}
                FOR EACH ROW EXECUTE PROCEDURE
                tsvector_update_trigger(search_name,
                                        'pg_catalog.english',
                                        name)
            '''.format(self.table_name)

            self.executeTransaction(trigger)

    def transform(self):
        for row in self.iterIncomingData():
            row = OrderedDict(zip(row.keys(), row.values()))

            # Replace status value
            if row['Status'] != 'A':
                row['Status'] = False
            else:
                row['Status'] = True

            # Replace position values
            for idx in ['CanSuppOpp', 'PolicySuppOpp']:
                if row[idx] == 'O':
                    row[idx] = 'oppose'
                elif row[idx] == 'S':
                    row[idx] = 'support'
                else:
                    row[idx] = None

            if row.get('TypeOfCommittee'):
                if 'Independent Expenditure' in row['TypeOfCommittee']:
                    row['TypeOfCommittee'] = 'Super PAC'

            yield OrderedDict(zip(self.header, list(row.values())))

class SunshineCandidates(SunshineTransformLoad):

    table_name = 'candidates'
    header = [
        f for f in Candidate.__table__.columns.keys()
        if f not in ['date_added', 'last_update', 'ocd_id']
    ]
    filename = 'Candidates.txt'


class SunshineOfficers(SunshineTransformLoad):
    table_name = 'officers'
    header = Officer.__table__.columns.keys()
    filename = 'Officers.txt'
    current = True

    def transform(self):
        for row in self.iterIncomingData():

            row_list = list(row.values())

            # Add empty committee_id
            row_list.insert(1, None)

            # Add empty resign date
            row_list.insert(11, None)

            # Add current flag
            row_list.append(self.current)

            yield OrderedDict(zip(self.header, row_list))

    def updateExistingRecords(self):
        ignore_fields = ['committee_id', 'resign_date', 'current']

        header = [f for f in self.header if f not in ignore_fields]

        fields = ','.join([
            '{0}=s."{1}"'.format(clean, raw)
            for clean, raw in zip(header, self.raw_header)
        ])

        update = '''
            UPDATE {table_name} SET
              {fields}
            FROM (
              SELECT * FROM raw_{table_name}
            ) AS s
            WHERE {table_name}.id = s."ID"
        '''.format(table_name=self.table_name,
                   fields=fields)

        self.executeTransaction(update)


class SunshinePrevOfficers(SunshineOfficers):
    table_name = 'officers'
    header = Officer.__table__.columns.keys()
    filename = 'PrevOfficers.txt'
    current = False

    def transform(self):
        for row in self.iterIncomingData():

            row_list = list(row.values())

            # Add empty phone
            row_list.insert(10, None)

            # Add current flag
            row_list.append(self.current)

            yield OrderedDict(zip(self.header, row_list))

    def updateExistingRecords(self):

        header = [f for f in self.header if f != 'phone']

        fields = ','.join([
            '{0}=s."{1}"'.format(clean, raw)
            for clean, raw in zip(header, self.raw_header)
        ])

        update = '''
            UPDATE {table_name} SET
              {fields}
            FROM (
              SELECT * FROM raw_{table_name}
              WHERE "ResignDate" IS NOT NULL
            ) AS s
            WHERE {table_name}.id = s."ID"
        '''.format(table_name=self.table_name,
                   fields=fields)

        self.executeTransaction(update)


class SunshineCandidacy(SunshineTransformLoad):
    table_name = 'candidacies'
    header = Candidacy.__table__.columns.keys()
    filename = 'CanElections.txt'

    election_types = {
        'CE': 'Consolidated Election',
        'GP': 'General Primary',
        'GE': 'General Election',
        'CP': 'Consolidated Primary',
        'NE': None,
        'SE': 'Special Election'
    }

    race_types = {
        'Inc': 'incumbent',
        'Open': 'open seat',
        'Chal': 'challenger',
        'Ret': 'retired',
    }

    def transform(self):
        for row in self.iterIncomingData():
            row = OrderedDict(zip(row.keys(), row.values()))

            # Get election type
            row['ElectionType'] = \
                self.election_types.get(row['ElectionType'].strip())

            # Get race type
            if row.get('IncChallOpen'):
                row['IncChallOpen'] = \
                    self.race_types.get(row['IncChallOpen'].strip())

            # Get outcome
            if row['WonLost'] == 'Won':
                row['WonLost'] = 'won'
            elif row['WonLost'] == 'Lost':
                row['WonLost'] = 'lost'
            else:
                row['WonLost'] = None

            yield OrderedDict(zip(self.header, row.values()))


class SunshineCandidateCommittees(SunshineTransformLoad):
    table_name = 'candidate_committees'
    header = ['committee_id', 'candidate_id']
    filename = 'CmteCandidateLinks.txt'

    def findNewRecords(self):
        create_new_record_table = '''
            CREATE TABLE new_{0} AS (
                SELECT
                  raw."CommitteeID",
                  raw."CandidateID"
                FROM raw_{0} AS raw
                LEFT JOIN {0} AS dat
                  ON raw."CommitteeID" = dat.committee_id
                  AND raw."CandidateID" = dat.candidate_id
                WHERE dat.committee_id IS NULL
                  AND dat.candidate_id IS NULL
            )
        '''.format(self.table_name)

        self.executeTransaction('DROP TABLE IF EXISTS new_{0}'.format(
            self.table_name
        ))
        self.executeTransaction(create_new_record_table)

    def iterIncomingData(self):
        incoming = '''
            SELECT raw.*
            FROM raw_{0} AS raw
            JOIN new_{0} AS new
              ON raw."CommitteeID" = new."CommitteeID"
              AND raw."CandidateID" = new."CandidateID"
        '''.format(self.table_name)

        for record in self.connection.engine.execute(incoming):
            yield record

    def transform(self):
        for row in self.iterIncomingData():
            row = [row['CommitteeID'], row['CandidateID']]
            yield OrderedDict(zip(self.header, row))


class SunshineOfficerCommittees(SunshineTransformLoad):
    table_name = 'officer_committees'
    header = ['committee_id', 'officer_id']
    filename = 'CmteOfficerLinks.txt'

    def findNewRecords(self):
        create_new_record_table = '''
            CREATE TABLE new_{0} AS (
                SELECT
                  raw."CommitteeID",
                  raw."OfficerID"
                FROM raw_{0} AS raw
                LEFT JOIN {0} AS dat
                  ON raw."CommitteeID" = dat.committee_id
                  AND raw."OfficerID" = dat.officer_id
                WHERE dat.committee_id IS NULL
                  AND dat.officer_id IS NULL
            )
        '''.format(self.table_name)

        self.executeTransaction('DROP TABLE IF EXISTS new_{0}'.format(
            self.table_name
        ))
        self.executeTransaction(create_new_record_table, rase_exc=True)

    def iterIncomingData(self):
        incoming = '''
            SELECT raw.*
            FROM raw_{0} AS raw
            JOIN new_{0} AS new
              ON raw."CommitteeID" = new."CommitteeID"
              AND raw."OfficerID" = new."OfficerID"
        '''.format(self.table_name)

        for record in self.connection.engine.execute(incoming):
            yield record

    def transform(self):
        for row in self.iterIncomingData():
            row = [row['CommitteeID'], row['OfficerID']]
            yield OrderedDict(zip(self.header, row))

    def updateExistingRecords(self):

        update = '''
            UPDATE officers SET
              committee_id=s."CommitteeID"
            FROM (
              SELECT * FROM raw_{table_name}
            ) AS s
            WHERE officers.id = s."OfficerID"
        '''.format(table_name=self.table_name)

        self.executeTransaction(update)


class SunshineD2Reports(SunshineTransformLoad):
    table_name = 'd2_reports'
    header = D2Report.__table__.columns.keys()
    filename = 'D2Totals.txt'


class SunshineFiledDocs(SunshineTransformLoad):
    table_name = 'filed_docs'
    header = FiledDoc.__table__.columns.keys()
    filename = 'FiledDocs.txt'


class SunshineReceipts(SunshineTransformLoad):
    table_name = 'receipts'
    header = Receipt.__table__.columns.keys()
    filename = 'Receipts.txt'

    # add receipt ids to omit because SBOE doesn't remove wrong data from db
    omit_receipt_ids = (4506842, 4513719)

    def delete_id_rows_from_receipts(self, omit_receipt_ids):
        """
        Deletes certain rows from master receipts datatable
        since SBOE doesn't remove wrong data from their db
        """
        for rid in omit_receipt_ids:

            del_sql = '''
                DELETE FROM receipts WHERE id = {0}
            '''.format(rid)
            self.executeTransaction(del_sql)

        return 'Successfully deleted omit rows from receipts table'


class SunshineExpenditures(SunshineTransformLoad):
    table_name = 'expenditures'
    header = Expenditure.__table__.columns.keys()
    filename = 'Expenditures.txt'


class SunshineInvestments(SunshineTransformLoad):
    table_name = 'investments'
    header = Investment.__table__.columns.keys()
    filename = 'Investments.txt'


class SunshineViews(object):

    def __init__(self, connection):
        self.connection = connection

    def executeTransaction(self, query, **kwargs):
        trans = self.connection.begin()

        try:
            rows = self.connection.execute(query, **kwargs)
            rows = [] if not rows or not rows.returns_rows else list(rows)
            trans.commit()
            return rows
        except (sa.exc.ProgrammingError, psycopg2.ProgrammingError) as e:
            #TODO: this line seems to break when creating views for the first time.
            # logger.error(e, exc_info=True)
            trans.rollback()
            raise e

    def first(self, query, **kwargs):
        result = self.executeTransaction(query, **kwargs)
        return result[0] if result else None

    def executeOutsideTransaction(self, query):

        self.connection.connection.set_isolation_level(0)
        curs = self.connection.connection.cursor()

        try:
            curs.execute(query)
        except psycopg2.ProgrammingError:
            pass

    def dropViews(self):
        print("Dropping receipts_by_month...")
        self.executeTransaction(
            'DROP MATERIALIZED VIEW IF EXISTS receipts_by_month'
        )

        print("Dropping committee_receipts_by_week...")
        self.executeTransaction(
            'DROP MATERIALIZED VIEW IF EXISTS committee_receipts_by_week'
        )

        print("Dropping incumbent_candidates...")
        self.executeTransaction(
            'DROP MATERIALIZED VIEW IF EXISTS incumbent_candidates'
        )

        print("Dropping most_recent_filings...")
        self.executeTransaction(
            'DROP MATERIALIZED VIEW IF EXISTS most_recent_filings CASCADE'
        )
        self.executeTransaction('DROP INDEX IF EXISTS most_recent_filings_idx CASCADE')
        self.executeTransaction('DROP INDEX IF EXISTS most_recent_filings_committee_received CASCADE')

        print("Dropping expenditures_by_candidate...")
        self.executeTransaction(
            'DROP MATERIALIZED VIEW IF EXISTS expenditures_by_candidate'
        )

        print("Dropping contested_races...")
        #self.executeTransaction('DROP TABLE IF EXISTS contested_races')

        print("Dropping muni_contested_races...")
        self.executeTransaction('DROP TABLE IF EXISTS muni_contested_races')

    def makeAllViews(self):
        print("Creating view - incumbent_candidates")
        self.incumbentCandidates()
        print("Creating view - most_recent_filings")
        self.mostRecentFilings()
        print("Creating view - condensed_receipts")
        self.condensedReceipts()
        print("Creating view - condensed_expenditures")
        self.condensedExpenditures()
        print("Creating view - expenditures_by_candidate")
        self.expendituresByCandidate()  # relies on condensed_expenditures
        print("Creating view - receipts_by_month")
        self.receiptsAggregates()  # relies on condensedReceipts
        print("Creating view - committee_receipts_by_week")
        self.committeeReceiptAggregates()  # relies on condensedReceipts
        print("Creating view - committee_money")
        self.committeeMoney()  # relies on mostRecentFilings
        print("Creating view - candidate_money")
        self.candidateMoney()  # relies on committeeMoney and mostRecentFilings
        print("Creating table - users_table")
        self.usersTable()
        print("Creating table - news_table")
        self.newsTable()
        print("Creating table - contested_races")
        self.contestedRaces()  # relies on sunshine/contested_races.csv, sunshine/gubernatorial_contested_races.csv, and sunshine/comptroller_contested_race.csv
        print("Creating table - muni_contested_races")
        self.muniContestedRaces()  # relies on sunshine/muni_contested_races.csv


    def condensedExpenditures(self):

        try:

            self.executeTransaction(
                'REFRESH MATERIALIZED VIEW CONCURRENTLY condensed_expenditures'
            )

        except sa.exc.ProgrammingError:

            rec = '''
                CREATE MATERIALIZED VIEW condensed_expenditures AS (
                  (
                    SELECT
                      e.*
                    FROM expenditures AS e
                    JOIN most_recent_filings AS m
                      USING(committee_id)
                    WHERE e.expended_date > COALESCE(m.reporting_period_end, '1900-01-01')
                      AND archived = FALSE
                  ) UNION (
                    SELECT
                      e.*
                    FROM expenditures AS e
                    JOIN (
                      SELECT DISTINCT ON (
                        reporting_period_begin,
                        reporting_period_end,
                        committee_id
                      )
                        id AS filed_doc_id
                      FROM filed_docs
                      WHERE doc_name != 'Pre-election'
                      ORDER BY reporting_period_begin,
                               reporting_period_end,
                               committee_id,
                               received_datetime DESC
                    ) AS f
                      USING(filed_doc_id)
                  )
                )
            '''
            self.executeTransaction(rec)

            self.condensedExpendituresIndex()

    def condensedReceipts(self):

        try:
            self.executeTransaction(
                'REFRESH MATERIALIZED VIEW CONCURRENTLY condensed_receipts'
            )

        except sa.exc.ProgrammingError:

            rec = '''
                CREATE MATERIALIZED VIEW condensed_receipts AS (
                  (
                    SELECT
                      r.*
                    FROM receipts AS r
                    LEFT JOIN most_recent_filings AS m
                      USING(committee_id)
                    WHERE r.received_date > COALESCE(m.reporting_period_end, '1900-01-01')
                      AND archived = FALSE
                  ) UNION (
                    SELECT
                      r.*
                    FROM receipts AS r
                    JOIN (
                      SELECT DISTINCT ON (
                        reporting_period_begin,
                        reporting_period_end,
                        committee_id
                      )
                        id AS filed_doc_id
                      FROM filed_docs
                      WHERE doc_name != 'Pre-election'
                      ORDER BY reporting_period_begin,
                               reporting_period_end,
                               committee_id,
                               received_datetime DESC
                    ) AS f
                      USING(filed_doc_id)
                  )
                )
            '''

            self.executeTransaction(rec)

            self.condensedReceiptsIndex()

    def expendituresByCandidate(self):

        try:
            self.executeTransaction(
                'REFRESH MATERIALIZED VIEW CONCURRENTLY expenditures_by_candidate'
            )

        except sa.exc.ProgrammingError:

            exp = '''
                CREATE MATERIALIZED VIEW expenditures_by_candidate AS (
                  SELECT
                    candidate_name,
                    office,
                    committee_id,
                    MAX(committee_name) AS committee_name,
                    MAX(committee_type) AS committee_type,
                    bool_or(supporting) AS supporting,
                    bool_or(opposing) AS opposing,
                    SUM(supporting_amount) AS supporting_amount,
                    SUM(opposing_amount) AS opposing_amount,
                    MIN(support_min_date) AS support_min_date,
                    MAX(support_max_date) AS support_max_date,
                    MIN(oppose_min_date) AS oppose_min_date,
                    MAX(oppose_max_date) AS oppose_max_date
                  FROM (
                    SELECT
                      e.candidate_name,
                      e.office,
                      cm.id AS committee_id,
                      MAX(cm.name) AS committee_name,
                      MAX(cm.type) AS committee_type,
                      bool_or(e.supporting) AS supporting,
                      bool_or(e.opposing) AS opposing,
                      SUM(e.amount) AS supporting_amount,
                      0 AS opposing_amount,
                      MIN(e.expended_date) AS support_min_date,
                      MAX(e.expended_date) AS support_max_date,
                      NULL::timestamp AS oppose_min_date,
                      NULL::timestamp AS oppose_max_date
                    FROM condensed_expenditures AS e
                    JOIN committees AS cm
                      ON e.committee_id = cm.id
                    WHERE supporting = TRUE AND opposing = FALSE
                      AND archived = FALSE
                    GROUP BY e.candidate_name, e.office, cm.id
                    UNION
                    SELECT
                      e.candidate_name,
                      e.office,
                      cm.id AS committee_id,
                      MAX(cm.name) AS committee_name,
                      MAX(cm.type) AS committee_type,
                      bool_or(e.supporting) AS supporting,
                      bool_or(e.opposing) AS opposing,
                      0 AS supporting_amount,
                      SUM(e.amount) AS opposing_amount,
                      NULL::timestamp AS support_min_date,
                      NULL::timestamp AS support_max_date,
                      MIN(e.expended_date) AS oppose_min_date,
                      MAX(e.expended_date) AS oppose_max_date
                    FROM condensed_expenditures AS e
                    JOIN committees AS cm
                      ON e.committee_id = cm.id
                    WHERE opposing = TRUE AND supporting = FALSE
                      AND archived = FALSE
                    GROUP BY e.candidate_name, e.office, cm.id
                  ) AS subq
                  GROUP BY candidate_name, office, committee_id
                )
            '''
            self.executeTransaction(exp)

            self.expendituresByCandidateIndex()

    def muniContestedRaces(self):
        """
        Creates the muni contested races view table from csv files hard
        saved in sunsine folder
        """
        try:
            races_input_file = csv.DictReader(open(
                os.getcwd() + '/sunshine/muni_contested_races.csv'
            ))

            entries = []
            for row in races_input_file:
                entries.append(row)

            contested_races = []
            for e in entries:
                supporting_funds = 0
                opposing_funds = 0
                controlled_amount = 0
                funds_available = 0
                contributions = 0
                total_funds = 0
                investments = 0
                debts = 0
                total_money = 0

                candidate_id = None

                try:
                    committee_id = int(float(e['ID']))
                except:
                    committee_id = None

                try:
                    district = e['District']
                except:
                    district = None

                first_name = e['First']
                last_name = e['Last']

                cand_name = first_name + " " + last_name

                supp_funds, opp_funds = \
                    self.ContestedRaces_get_candidate_funds_byname(cand_name)
                supporting_funds = supporting_funds + supp_funds
                opposing_funds = opposing_funds + opp_funds

                if committee_id:
                    committee, recent_receipts, recent_total, latest_filing, controlled_amount, ending_funds, investments, debts, expenditures, total_expenditures = self.ContestedRaces_get_committee_details(committee_id)

                    funds_available = latest_filing['end_funds_available'] if latest_filing else 0
                    contributions = recent_total
                    total_funds = controlled_amount
                    investments = latest_filing['total_investments'] if latest_filing else 0
                    debts = latest_filing['total_debts'] if latest_filing else 0


                total_money = \
                    supporting_funds + opposing_funds + controlled_amount
                contested_races.append({
                    'district': district,
                    'branch': e['Senate/House'],
                    'last_name': last_name,
                    'first_name': first_name,
                    'committee_name': e['Committee'],
                    'incumbent': e['Incumbent'],
                    'committee_id': committee_id,
                    'party': e['Party'],
                    'funds_available': funds_available,
                    'contributions': contributions,
                    'total_funds': total_funds,
                    'investments': investments,
                    'debts': debts,
                    'supporting_funds': supporting_funds,
                    'opposing_funds': opposing_funds,
                    'candidate_id': candidate_id,
                    'total_money': total_money,
                    'reporting_period_end': latest_filing['reporting_period_end'] if latest_filing else None
                })

            exp = '''
                CREATE TABLE muni_contested_races(
                    total_money DOUBLE PRECISION,
                    branch TEXT,
                    last_name TEXT,
                    first_name TEXT,
                    committee_name TEXT,
                    incumbent TEXT,
                    committee_id INTEGER,
                    party TEXT,
                    funds_available DOUBLE PRECISION,
                    contributions DOUBLE PRECISION,
                    total_funds DOUBLE PRECISION,
                    investments DOUBLE PRECISION,
                    debts DOUBLE PRECISION,
                    supporting_funds DOUBLE PRECISION,
                    opposing_funds DOUBLE PRECISION,
                    reporting_period_end DATE,
                    district TEXT,
                    candidate_id INTEGER
                )
            '''

            trans = self.connection.begin()
            curs = self.connection.connection.cursor()
            curs.execute(exp)
            insert_statement = \
                'INSERT INTO muni_contested_races (%s) VALUES %s'
            cols = [
                'last_name',
                'committee_id',
                'incumbent',
                'district',
                'first_name',
                'total_funds',
                'candidate_id',
                'investments',
                'committee_name',
                'supporting_funds',
                'opposing_funds',
                'party',
                'branch',
                'contributions',
                'debts',
                'total_money',
                'funds_available',
                'reporting_period_end'
            ]
            for cr in contested_races:

                values = [cr[column] for column in cols]
                curs.execute(
                    insert_statement, (AsIs(','.join(cols)), tuple(values))
                )

            trans.commit()
        except sa.exc.ProgrammingError:
            print('Problem in creating muni contested_races table')

    def contestedRaces(self):
        """
        Creates the contested_races table.
        """

        try:
            exp = '''
                CREATE TABLE IF NOT EXISTS contested_races(
                    total_money DOUBLE PRECISION,
                    branch TEXT,
                    last_name TEXT,
                    first_name TEXT,
                    committee_name TEXT,
                    incumbent TEXT,
                    committee_id INTEGER,
                    party TEXT,
                    funds_available DOUBLE PRECISION,
                    contributions DOUBLE PRECISION,
                    total_funds DOUBLE PRECISION,
                    investments DOUBLE PRECISION,
                    debts DOUBLE PRECISION,
                    supporting_funds DOUBLE PRECISION,
                    opposing_funds DOUBLE PRECISION,
                    reporting_period_end DATE,
                    district INTEGER,
                    candidate_id INTEGER,
                    alternate_names TEXT
                )
            '''
            self.executeTransaction(sa.text(exp))
        except (psycopg2.ProgrammingError, sa.exc.ProgrammingError):
            print('Problem in creating contested_races table: ')
            print(traceback.print_exc())

        try:
            self.executeTransaction(sa.text('''ALTER TABLE contested_races ADD COLUMN id SERIAL PRIMARY KEY'''))
        except (psycopg2.ProgrammingError, sa.exc.ProgrammingError):
            pass

        try:
            self.executeTransaction(sa.text('''ALTER TABLE contested_races ADD COLUMN district_name varchar(50)'''))
        except (psycopg2.ProgrammingError, sa.exc.ProgrammingError):
            pass

        # Update the contested races data.
        self.ContestedRaces_updateContestedRacesFunds()

    #============================================================================
    def ContestedRaces_updateContestedRacesFunds(self, races = []):

        # Get candidate funds data
        try:
            races_sql = '''SELECT * FROM contested_races'''
            races = self.executeTransaction(sa.text(races_sql))
        except (psycopg2.ProgrammingError, sa.exc.ProgrammingError):
            print('Problem retrieving contested_races data: ')
            traceback.print_exc()
            return
        except Exception:
            print('Unknown problem retrieving contested_races data: ')
            traceback.print_exc()
            return

        for race in races:
            self.ContestedRaces_updateContestedRaceFunds(race)

    #============================================================================
    def ContestedRaces_updateContestedRaceFunds(self, race):
        race_data = None

        if not race:
            return

        try:
            id = race.id
            primary_details = self.ContestedRaces_getPrimaryDetails(race.branch)
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
                supp_funds, opp_funds = self.ContestedRaces_get_candidate_funds_byname(name)
                supporting_funds += supp_funds
                opposing_funds += opp_funds

            # Get candidate contested race funds
            if race.committee_id:
                total_funds = self.ContestedRaces_getCommitteeFundsData(race.committee_id, pre_primary_start, primary_start, post_primary_start)
                funds_available = self.ContestedRaces_getFundsRaisedTotal(race.committee_id, pre_primary_start, primary_start, primary_end) if is_after_primary else 0

                committee, recent_receipts, recent_total, latest_filing, controlled_amount, ending_funds, investments, debts, expenditures, total_expenditures = self.ContestedRaces_get_committee_details(race.committee_id)
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
            print('Problem calculating contested_race funds data: ')
            print(traceback.print_exc())

        if not race_data:
            return

        # Update candidate funds data
        try:
            self.executeTransaction(sa.text('''
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
            print('Problem updating contested_races funds data: ')
            print(traceback.print_exc())

    #============================================================================
    def ContestedRaces_getPrimaryDetails(self, branch):
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
    def ContestedRaces_get_candidate_funds_byname(self, candidate_name):
        d2_part = '9B'
        expended_date = datetime(2017, 1, 1, 0, 0)
        supporting_funds = 0
        opposing_funds = 0

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
            result = self.first(
                sa.text(supporting_funds_sql),
                candidate_name=candidate_name,
                d2_part=d2_part,
                expended_date=expended_date
            )

            if result is not None:
                supporting_funds = result.amount
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
            result = self.first(
                sa.text(opposing_funds_sql),
                candidate_name=candidate_name,
                d2_part=d2_part,
                expended_date=expended_date
            )

            if result is not None:
                opposing_funds = result.amount
        except (sa.exc.ProgrammingError, psycopg2.ProgrammingError) as e:
            raise e

        return supporting_funds, opposing_funds

    #============================================================================
    def ContestedRaces_getCommitteeFundsData(self, committee_id, pre_primary_start, primary_start, post_primary_start):
        current_date = datetime.now().strftime("%Y-%m-%d")
        total_funds_raised = 0.0
        primary_funds_raised = None
        # Date used to eliminate duplicates
        temp_dtstart = ""

        sql = """SELECT
                    d.total_receipts,
                    d.total_inkind,
                    d.end_funds_available,
                    fd.reporting_period_begin,
                    fd.reporting_period_end
                 FROM d2_reports d
                 JOIN filed_docs fd on (fd.id = d.filed_doc_id)
                 WHERE
                    d.archived = FALSE AND
                    fd.archived = FALSE AND
                    d.committee_id = :committee_id AND
                    fd.doc_name = 'Quarterly' AND
                    fd.reporting_period_begin >= :dt_start AND
                    fd.reporting_period_end <= :dt_end
                 ORDER BY fd.reporting_period_begin, fd.reporting_period_end, fd.received_datetime DESC"""

        pre_primary_quarterlies = self.executeTransaction(sa.text(sql),
            committee_id=committee_id,
            dt_start=pre_primary_start,
            dt_end=primary_start)

        primary_quarterlies = self.executeTransaction(sa.text(sql),
            committee_id=committee_id,
            dt_start=primary_start,
            dt_end=current_date)

        if current_date >= post_primary_start:
            if primary_quarterlies:
                # Add rows for each primary quarterly report.
                for i, rpt in enumerate(primary_quarterlies):

                    # If the reporting period begin date equals the temporary start date, skip the record
                    if temp_dtstart == rpt["reporting_period_begin"]:
                        continue

                    temp_dtstart = rpt["reporting_period_begin"]
                    rpt_total = rpt["total_receipts"] + rpt["total_inkind"]

                    # For the first quarterly (the primary quarterly) use the end_funds_available.
                    if i == 0:
                        rpt_total = rpt["end_funds_available"]

                    last_quarterly_date = rpt["reporting_period_end"]
                    total_funds_raised += rpt_total

                # Add a row for funds raised since the last primary quarterly report (pulled from Receipt).
                total_receipts = self.ContestedRaces_getReceiptsTotal(committee_id, "A-1", last_quarterly_date, current_date)
                total_funds_raised += total_receipts
            else:
                if pre_primary_quarterlies:
                    # Add the funds available from the last pre-primary quarterly report.
                    pre_primary_end_date = "{dt:%b} {dt.day}, {dt.year}".format(dt = pre_primary_quarterlies[-1]["reporting_period_end"])
                    total_funds_raised = pre_primary_quarterlies[-1]["end_funds_available"]
                    last_quarterly_date = pre_primary_quarterlies[-1]["reporting_period_end"]
                else:
                    total_funds_raised = 0.0
                    last_quarterly_date = parse(pre_primary_start)

                # Add contributions since last quaterly report.
                total_receipts = self.ContestedRaces_getReceiptsTotal(committee_id, "A-1", last_quarterly_date, current_date)
                total_funds_raised += total_receipts

        else:
            # Default the last quarterly date to the day before the pre-primary starts.
            last_quarterly_date = parse(pre_primary_start) - timedelta(days=1)

            pre_pre_primary_sql = """
                     SELECT
                        d.total_receipts,
                        d.end_funds_available,
                        fd.reporting_period_begin,
                        fd.reporting_period_end
                     FROM d2_reports d
                     JOIN filed_docs fd on (fd.id = d.filed_doc_id)
                     WHERE
                        d.archived = FALSE AND
                        fd.archived = FALSE AND
                        d.committee_id = :committee_id AND
                        fd.doc_name = 'Quarterly' AND
                        fd.reporting_period_end <= :dt_end
                     ORDER BY fd.reporting_period_begin DESC, fd.reporting_period_end, fd.received_datetime DESC
                     LIMIT 1"""

            pre_pre_primary_quarterly = self.first(sa.text(pre_pre_primary_sql),
                committee_id=committee_id,
                dt_end=last_quarterly_date
            )

            # Add the funds available from the last quarterly report from before the pre-primary start date.
            pre_pre_primary_end_date = "{dt:%b} {dt.day}, {dt.year}".format(dt = last_quarterly_date)
            total_funds_raised += 0 if not pre_pre_primary_quarterly else pre_pre_primary_quarterly["end_funds_available"]

            # Add rows for each pre-primary quarterly report.
            for rpt in pre_primary_quarterlies:
                # If the reporting period being date equals the temporary start date, skip the record
                if temp_dtstart != rpt["reporting_period_begin"]:
                    temp_dtstart = rpt["reporting_period_begin"]
                    last_quarterly_date = rpt["reporting_period_end"]
                    total_funds_raised += rpt["total_receipts"] + rpt["total_inkind"]

            # Add funds raised since last quaterly report.
            total_receipts = self.ContestedRaces_getReceiptsTotal(committee_id, "A-1", last_quarterly_date, current_date)
            total_funds_raised += total_receipts if total_receipts else 0.0

        return total_funds_raised

    #============================================================================
    def ContestedRaces_getReceiptsTotal(self, committee_id, doc_name, last_period_end, last_receipt_date=None):
        sql = """
             SELECT SUM(r.amount) as amount
             FROM receipts r
             JOIN filed_docs fd on (fd.id = r.filed_doc_id)
             WHERE
                r.archived = FALSE AND
                r.committee_id = :committee_id AND
                fd.archived = FALSE AND
                fd.doc_name = :doc_name AND
                fd.reporting_period_begin >= :last_period_end"""

        query_params = {
            "committee_id": committee_id,
            "doc_name": doc_name,
            "last_period_end": last_period_end
        }

        if last_receipt_date:
            sql += " AND fd.received_datetime <= :last_receipt_date"
            query_params["last_receipt_date"] = last_receipt_date

        total_receipts = self.first(sa.text(sql), **query_params)
        return total_receipts["amount"] if total_receipts is not None else 0.0


    #============================================================================
    def ContestedRaces_getFundsRaisedTotal(self, committee_id, quarterly_start_date, next_quarterly_start_date, receipt_end_date):
        sql = """
             SELECT SUM(d.total_receipts) as total
             FROM d2_reports d
             JOIN filed_docs fd on (fd.id = d.filed_doc_id)
             WHERE
                d.archived = FALSE AND
                d.committee_id = :committee_id AND
                fd.archived = FALSE AND
                fd.doc_name = 'Quarterly' AND
                fd.reporting_period_begin >= :dt_start AND
                fd.reporting_period_end >= :dt_end"""

        result = self.first(sa.text(sql),
            committee_id=commitee_id,
            dt_start=quarterly_start_date,
            dt_end=next_quarterly_start_date)
        pre_primary_total_raised = result.total if result is not None else 0.0
        quarterly_end_date = parse(next_quarterly_start_date) - timedelta(days=1)
        contributions = 0

        # Add contributions since last quarterly report.
        if pre_primary_total_raised is None:
            # If there is no Quarterly report yet, then set pre_primary raised
            # amount to 0 and get the A-1 amound since the quarterly_start_date.
            pre_primary_total_raised = 0
            contributions = self.ContestedRaces_getReceiptsTotal(committee_id, "A-1", quarterly_start_date, receipt_end_date)
        else:
            contributions = self.ContestedRaces_getReceiptsTotal(committee_id, "A-1", quarterly_end_date, receipt_end_date)

        return pre_primary_total_raised + contributions

    #============================================================================
    def ContestedRaces_get_committee_details(self, committee_id):
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
        committee = self.first(
            sa.text(comm_sql),
            committee_id=committee_id)

        if not committee:
            return default_return

        latest_filing = '''(
            SELECT * FROM most_recent_filings
            WHERE committee_id = :committee_id
            ORDER BY received_datetime DESC
            LIMIT 1
            )
        '''

        latest_filing = dict(self.first(
            sa.text(latest_filing),
            committee_id=committee_id))

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
                  AND receipts.archived = FALSE
                  AND filed.archived = FALSE
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
                    AND receipts.archived = FALSE
                    AND filed.archived = FALSE
                )
            '''

            controlled_amount = 0

        recent_total = self.first(
            sa.text(recent_receipts),
            **params
        )["amount"]
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
              AND r.archived = FALSE
              AND f.archived = FALSE
            ORDER BY f.reporting_period_end ASC
            )
        '''

        quarterlies = self.executeTransaction(
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
    def usersTable(self):
        try:
            trans = self.connection.begin()
            curs = self.connection.connection.cursor()

            exp = '''
                CREATE TABLE IF NOT EXISTS users_table (
                    id SERIAL NOT NULL PRIMARY KEY,
                    username VARCHAR(255) UNIQUE NOT NULL,
                    email VARCHAR(255),
                    password VARCHAR(100) NOT NULL,
                    is_active BOOLEAN,
                    is_admin BOOLEAN,
                    created_date TIMESTAMP,
                    updated_date TIMESTAMP
                )
            '''

            curs.execute(exp)
            trans.commit()
        except (sa.exc.ProgrammingError, psycopg2.ProgrammingError) as e:
            trans.rollback()
            print('Problem in creating users_table table: ')
            print(traceback.print_exc())
            logger.error(e, exc_info=True)

    def newsTable(self):

        try:
            trans = self.connection.begin()
            curs = self.connection.connection.cursor()

            exp = '''
                CREATE TABLE IF NOT EXISTS news_table (
                    id SERIAL NOT NULL PRIMARY KEY,
                    key VARCHAR(50) UNIQUE NOT NULL,
                    content TEXT,
                    created_date TIMESTAMP,
                    updated_date TIMESTAMP
                )
            '''


            curs.execute(exp)
            trans.commit()

        except (sa.exc.ProgrammingError, psycopg2.ProgrammingError) as e:
            trans.rollback()
            print('Problem in creating news_table table: ')
            print(traceback.print_exc())
            logger.error(e, exc_info=True)

    def receiptsAggregates(self):

        try:

            self.executeTransaction(
                'REFRESH MATERIALIZED VIEW CONCURRENTLY receipts_by_month'
            )

        except sa.exc.ProgrammingError:

            weeks = '''
                CREATE MATERIALIZED VIEW receipts_by_month AS (
                  SELECT
                    date_trunc('month', received_date) AS month,
                    SUM(amount) AS total_amount,
                    COUNT(id) AS donation_count,
                    AVG(amount) AS average_donation
                  FROM condensed_receipts
                  GROUP BY date_trunc('month', received_date)
                  ORDER BY month
                )
            '''
            self.executeTransaction(weeks)

            self.receiptsByWeekIndex()

    def committeeReceiptAggregates(self):

        try:
            self.executeTransaction(
                'REFRESH MATERIALIZED VIEW CONCURRENTLY committee_receipts_by_week'
            )

        except sa.exc.ProgrammingError:

            weeks = '''
                CREATE MATERIALIZED VIEW committee_receipts_by_week AS (
                  SELECT
                    committee_id,
                    date_trunc('week', received_date) AS week,
                    SUM(amount) AS total_amount,
                    COUNT(id) AS donation_count,
                    AVG(amount) AS average_donation
                  FROM receipts
                  GROUP BY committee_id,
                           date_trunc('week', received_date)
                  ORDER BY week
                )

            '''

            self.executeTransaction(weeks)

            self.committeeReceiptsByWeekIndex()

    def incumbentCandidates(self):

        try:

            self.executeTransaction(
                'REFRESH MATERIALIZED VIEW CONCURRENTLY incumbent_candidates'
            )

        except (sa.exc.ProgrammingError, psycopg2.ProgrammingError):

            incumbents = '''
                CREATE MATERIALIZED VIEW incumbent_candidates AS (
                  SELECT DISTINCT ON (cd.district, cd.office)
                    cd.*,
                    cs.election_year AS last_election_year,
                    cs.election_type AS last_election_type,
                    cs.race_type AS last_race_type
                  FROM candidates AS cd
                  JOIN candidacies AS cs
                    ON cd.id = cs.candidate_id
                  WHERE cs.outcome = :outcome
                    AND cs.election_year >= :year
                  ORDER BY cd.district, cd.office, cs.id DESC
                )
            '''

            last_year = datetime.now().year - 1

            self.executeTransaction(sa.text(incumbents),
                                    outcome='won',
                                    year=last_year)

            self.incumbentCandidatesIndex()

    def mostRecentFilings(self):

        try:

            self.executeTransaction(
                'REFRESH MATERIALIZED VIEW CONCURRENTLY most_recent_filings'
            )

        except sa.exc.ProgrammingError:

            create = '''
               CREATE MATERIALIZED VIEW most_recent_filings AS (
                 SELECT
                   COALESCE(d2.end_funds_available, 0) AS end_funds_available,
                   COALESCE(d2.total_investments, 0) AS total_investments,
                   COALESCE(d2.total_debts, 0) AS total_debts,
                   COALESCE((d2.inkind_itemized + d2.inkind_non_itemized), 0) AS total_inkind,
                   cm.name AS committee_name,
                   cm.id AS committee_id,
                   cm.type AS committee_type,
                   cm.active AS committee_active,
                   fd.id AS filed_doc_id,
                   fd.doc_name,
                   fd.reporting_period_end,
                   fd.reporting_period_begin,
                   fd.received_datetime
                 FROM committees AS cm
                 LEFT JOIN (
                   SELECT DISTINCT ON (committee_id)
                     f.*
                   FROM (
                     SELECT DISTINCT ON (committee_id, reporting_period_end)
                       id,
                       committee_id,
                       doc_name,
                       reporting_period_end,
                       reporting_period_begin,
                       received_datetime
                     FROM filed_docs
                     WHERE doc_name NOT IN (
                       'A-1',
                       'Statement of Organization',
                       'Letter/Correspondence',
                       'B-1',
                       'Nonparticipation'
                     )
                     ORDER BY committee_id,
                              reporting_period_end DESC,
                              received_datetime DESC
                   ) AS f
                   ORDER BY f.committee_id,
                            f.reporting_period_end DESC
                 ) AS fd
                   ON fd.committee_id = cm.id
                 LEFT JOIN d2_reports AS d2
                   ON fd.id = d2.filed_doc_id
               )
            '''
            self.executeTransaction(create)

            self.mostRecentFilingsIndex()

    def committeeMoney(self):

        try:

            self.executeTransaction(
                'REFRESH MATERIALIZED VIEW CONCURRENTLY committee_money'
            )

        except sa.exc.ProgrammingError:
            create = '''
               CREATE MATERIALIZED VIEW committee_money AS (
                 SELECT
                   MAX(filings.end_funds_available) AS end_funds_available,
                   MAX(filings.total_inkind) AS total_inkind,
                   MAX(filings.committee_name) AS committee_name,
                   MAX(filings.committee_id) AS committee_id,
                   MAX(filings.committee_type) AS committee_type,
                   bool_and(filings.committee_active) AS committee_active,
                   MAX(filings.doc_name) AS doc_name,
                   MAX(filings.reporting_period_end) AS reporting_period_end,
                   MAX(filings.reporting_period_begin) AS reporting_period_begin,
                   (SUM(COALESCE(receipts.amount, 0)) +
                    MAX(COALESCE(filings.end_funds_available, 0))) AS total,
                   MAX(receipts.received_date) AS last_receipt_date
                 FROM most_recent_filings AS filings
                 LEFT JOIN receipts
                   ON receipts.committee_id = filings.committee_id
                   AND receipts.received_date > LEAST(COALESCE(filings.reporting_period_end, :end_date))
                    AND receipts.archived = FALSE
                 GROUP BY filings.committee_id
                 ORDER BY total DESC NULLS LAST
               )
            '''
            # set end date in case reporting period end empty
            end_date = datetime.now() - timedelta(days=90)
            self.executeTransaction(sa.text(create), end_date=end_date)

            self.committeeMoneyIndex()

    def candidateMoney(self):

        try:

            self.executeTransaction(
                'REFRESH MATERIALIZED VIEW CONCURRENTLY candidate_money'
            )

        except sa.exc.ProgrammingError:

            create = '''
                CREATE MATERIALIZED VIEW candidate_money AS (
                  SELECT
                    cd.id AS candidate_id,
                    cd.first_name AS candidate_first_name,
                    cd.last_name AS candidate_last_name,
                    cd.office AS candidate_office,
                    cm.id AS committee_id,
                    cm.name AS committee_name,
                    cm.type AS committee_type,
                    m.total,
                    m.last_receipt_date
                  FROM candidates AS cd
                  JOIN candidate_committees AS cc
                    ON cd.id = cc.candidate_id
                  JOIN committees AS cm
                    ON cc.committee_id = cm.id
                  JOIN committee_money AS m
                    ON cm.id = m.committee_id
                  ORDER BY m.total DESC NULLS LAST
                )
            '''
            self.executeTransaction(create)

            self.candidateMoneyIndex()

    def makeUniqueIndexes(self):
        '''
        Need a unique index on materialized views so that
        can be refreshed concurrently
        '''
        self.condensedExpendituresIndex()
        self.condensedReceiptsIndex()
        self.condensedReceiptsDateIndex()
        self.condensedExpendituresDateIndex()
        self.expendituresByCandidateIndex()
        self.receiptsByWeekIndex()
        self.committeeReceiptsByWeekIndex()
        self.incumbentCandidatesIndex()
        self.candidateMoneyIndex()
        self.committeeMoneyIndex()
        self.mostRecentFilingsIndex()

    def condensedExpendituresIndex(self):
        index = '''
            CREATE UNIQUE INDEX CONCURRENTLY condensed_expenditures_id_idx
            ON condensed_expenditures(id)
        '''

        self.executeOutsideTransaction(index)

    def condensedReceiptsIndex(self):
        index = '''
            CREATE UNIQUE INDEX CONCURRENTLY condensed_receipts_id_idx
            ON condensed_receipts(id)
        '''

        self.executeOutsideTransaction(index)

        index = '''
            CREATE INDEX CONCURRENTLY condensed_receipts_committee_received_doc_amount
            ON condensed_receipts(committee_id, received_date, filed_doc_id, amount)
        '''

        self.executeOutsideTransaction(index)

    def condensedExpendituresDateIndex(self):
        index = '''
            CREATE INDEX CONCURRENTLY condensed_expenditures_date_idx
            ON condensed_expenditures(expended_date)
        '''

        self.executeOutsideTransaction(index)

    def condensedReceiptsDateIndex(self):
        index = '''
            CREATE INDEX CONCURRENTLY condensed_receipts_date_idx
            ON condensed_receipts(date)
        '''

        self.executeOutsideTransaction(index)

    def expendituresByCandidateIndex(self):
        index = '''
            CREATE UNIQUE INDEX CONCURRENTLY expenditures_by_candidate_idx
            ON expenditures_by_candidate(candidate_name, office, committee_id, supporting)
        '''

        self.executeOutsideTransaction(index)

    def receiptsByWeekIndex(self):
        index = '''
            CREATE UNIQUE INDEX CONCURRENTLY receipts_by_month_idx
            ON receipts_by_month(month)
        '''

        self.executeOutsideTransaction(index)

    def committeeReceiptsByWeekIndex(self):
        index = '''
            CREATE UNIQUE INDEX CONCURRENTLY committee_receipts_by_week_idx
            ON committee_receipts_by_week(committee_id, week)
        '''

        self.executeOutsideTransaction(index)

    def incumbentCandidatesIndex(self):

        index = '''
            CREATE UNIQUE INDEX CONCURRENTLY incumbent_candidates_idx
            ON incumbent_candidates(id)
        '''

        self.executeOutsideTransaction(index)

    def candidateMoneyIndex(self):
        index = '''
            CREATE UNIQUE INDEX CONCURRENTLY candidate_money_idx
            ON candidate_money(candidate_id, committee_id)
        '''

        self.executeOutsideTransaction(index)

    def committeeMoneyIndex(self):
        index = '''
            CREATE UNIQUE INDEX CONCURRENTLY committee_money_idx
            ON committee_money(committee_id)
        '''

        self.executeOutsideTransaction(index)

    def mostRecentFilingsIndex(self):
        index = '''
            CREATE UNIQUE INDEX CONCURRENTLY most_recent_filings_idx
            ON most_recent_filings(committee_id, reporting_period_end)
        '''

        self.executeOutsideTransaction(index)

        index = '''
            CREATE INDEX CONCURRENTLY most_recent_filings_committee_received
            ON most_recent_filings(committee_id, received_datetime)
        '''

        self.executeOutsideTransaction(index)


class SunshineIndexes(object):
    def __init__(self, connection):
        self.connection = connection

    def executeTransaction(self, query):
        trans = self.connection.begin()

        try:
            self.connection.execute(query)
            trans.commit()
        except sa.exc.ProgrammingError:
            trans.rollback()

    def executeOutsideTransaction(self, query):

        self.connection.connection.set_isolation_level(0)
        curs = self.connection.connection.cursor()

        try:
            curs.execute(query)
        except psycopg2.ProgrammingError:
            pass

    def makeAllIndexes(self):
        print("Creating indices...")
        self.receiptsDate()
        self.receiptsCommittee()
        self.receiptsFiledDocs()
        self.candidaciesCandidate()
        self.candidateCommittees()
        self.officersCommittee()
        self.filedDocsCommittee()
        self.receiptsName()
        self.receiptsDateAmount()
        self.expendituresName()

    def receiptsDate(self):
        '''
        Make index on received_date for receipts
        '''
        index = '''
            CREATE INDEX CONCURRENTLY received_date_idx ON receipts (received_date)
        '''

        self.executeOutsideTransaction(index)

    def receiptsCommittee(self):
        '''
        Make index on committee_id for receipts
        '''
        index = '''
            CREATE INDEX CONCURRENTLY receipts_committee_idx ON receipts (committee_id)
        '''

        self.executeOutsideTransaction(index)

    def receiptsFiledDocs(self):
        index = '''
            CREATE INDEX CONCURRENTLY receipts_filed_docs_idx ON receipts (filed_doc_id)
        '''

        self.executeOutsideTransaction(index)

    def candidaciesCandidate(self):
        index = '''
            CREATE INDEX CONCURRENTLY candidacies_candidate_id_index
              ON candidacies (candidate_id)
        '''

        self.executeOutsideTransaction(index)

    def candidateCommittees(self):
        index = '''
            CREATE INDEX CONCURRENTLY cand_comm_candidate_id_index
              ON candidate_committees (candidate_id)
        '''

        self.executeOutsideTransaction(index)

        index = '''
            CREATE INDEX CONCURRENTLY cand_comm_committee_id_index
              ON candidate_committees (committee_id)
        '''

        self.executeOutsideTransaction(index)

    def filedDocsCommittee(self):
        index = '''
            CREATE INDEX CONCURRENTLY filed_docs_committee_idx ON filed_docs (committee_id)
        '''

        self.executeOutsideTransaction(index)

    def officersCommittee(self):
        index = '''
            CREATE INDEX CONCURRENTLY officers_committee_id_index
              ON officers (committee_id)
        '''

        self.executeOutsideTransaction(index)

    def receiptsName(self):
        index = '''
             CREATE INDEX CONCURRENTLY condensed_receipts_search_index ON condensed_receipts
             USING gin(search_name)
        '''

        self.executeOutsideTransaction(index)

    def receiptsDateAmount(self):
        index = '''
             CREATE INDEX CONCURRENTLY condensed_receipts_date_amounts_index ON condensed_receipts
             USING gin(received_date, amount)
        '''

        self.executeOutsideTransaction(index)

    def expendituresName(self):
        index = '''
             CREATE INDEX CONCURRENTLY condensed_expenditures_search_index ON condensed_expenditures
             USING gin(search_name)
        '''

        self.executeOutsideTransaction(index)


def downloadUnzip():
    import urllib
    import zipfile

    latest_filename = 'IL_Campaign_Disclosure_latest.zip'
    download_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), 'downloads')
    )

    download_location = os.path.join(download_path, latest_filename)

    download_url = 'http://il-elections.s3.amazonaws.com/%s' % latest_filename

    filename, _ = urllib.request.urlretrieve(download_url,
                                             filename=download_location)

    with zipfile.ZipFile(filename, 'r') as zf:
        # date_prefix = zf.namelist()[0].split('/')[0]
        zf.extractall(path=download_path)

    for dir_member in os.listdir(os.path.join(download_path)):
        dir_path = os.path.join(download_path, dir_member)
        if (not os.path.isdir(dir_path)):
            continue
        for member in os.listdir(dir_path):
            move_from = os.path.join(dir_path, member)
            move_to = os.path.join(download_path, member)
            os.rename(move_from, move_to)

def alterSearchDictionary():
    from sunshine.app_config import DB_HOST, DB_PORT, DB_NAME, STOP_WORD_LIST

    alter = '''
        ALTER TEXT SEARCH DICTIONARY english_stem (StopWords = '{0}');
    '''.format(STOP_WORD_LIST)

    DB_USER = 'postgres'
    DB_PW = ''

    DB_CONN = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(
        DB_USER, DB_PW, DB_HOST, DB_PORT, DB_NAME
    )

    engine = sa.create_engine(DB_CONN,
                              convert_unicode=True,
                              server_side_cursors=True)

    with engine.begin() as conn:
        conn.execute(alter)

def main(args):
    from sunshine.app_config import STOP_WORD_LIST
    from sunshine.database import engine, Base

    connection = engine.connect()

    if args.download:
        print("download start %s ..." % datetime.now().isoformat())
        logger.info("download start %s ..." % datetime.now().isoformat())

        downloadUnzip()

        logger.info("download finish %s ..." % datetime.now().isoformat())
        print("download finish %s ..." % datetime.now().isoformat())
    else:
        print("skipping download")

    if args.load_data:
        print("loading data start %s ..." % datetime.now().isoformat())

        if STOP_WORD_LIST != 'english':
            alterSearchDictionary()

        chunk_size = 50000

        if args.chunk_size:
            chunk_size = args.chunk_size

        committees = SunshineCommittees(connection,
                                        Base.metadata,
                                        chunk_size=chunk_size)
        committees.load(update_existing=True)
        committees.addNameColumn()
        committees.addDateColumn('NULL')

        del committees
        del Base.metadata

        candidates = SunshineCandidates(connection, chunk_size=chunk_size)
        candidates.load(update_existing=True)
        candidates.addNameColumn()
        candidates.addDateColumn('NULL')

        del candidates

        officers = SunshineOfficers(connection, chunk_size=chunk_size)
        officers.load(update_existing=True)
        officers.addNameColumn()
        officers.addDateColumn('NULL')

        del officers

        #prev_off = SunshinePrevOfficers(connection, chunk_size=chunk_size)
        #prev_off.load(update_existing=True)

        #del prev_off

        candidacy = SunshineCandidacy(connection, chunk_size=chunk_size)
        candidacy.load()

        del candidacy

        can_cmte_xwalk = \
            SunshineCandidateCommittees(connection, chunk_size=chunk_size)
        can_cmte_xwalk.load()

        del can_cmte_xwalk

        off_cmte_xwalk = \
            SunshineOfficerCommittees(connection, chunk_size=chunk_size)
        off_cmte_xwalk.load(update_existing=True)

        del off_cmte_xwalk

        filed_docs = SunshineFiledDocs(connection, chunk_size=chunk_size)
        filed_docs.load()

        del filed_docs

        d2_reports = SunshineD2Reports(connection, chunk_size=chunk_size)
        d2_reports.load()

        del d2_reports

        receipts = SunshineReceipts(connection, chunk_size=chunk_size)
        receipts.load()
        receipts.addNameColumn()
        receipts.addDateColumn('received_date')

        # delete specific rows from receipts table since
        # sboe doesn't remove wrong data from db
        receipts.delete_id_rows_from_receipts(receipts.omit_receipt_ids)

        del receipts

        expenditures = SunshineExpenditures(connection, chunk_size=chunk_size)
        expenditures.load()
        expenditures.addNameColumn()
        expenditures.addDateColumn('expended_date')

        del expenditures

        investments = SunshineInvestments(connection, chunk_size=chunk_size)
        investments.load()
        investments.addNameColumn()
        investments.addDateColumn('purchase_date')

        del investments

        print("loading data end %s ..." % datetime.now().isoformat())

    else:
        print("skipping load")

    views = SunshineViews(connection)

    if args.recreate_views:
        print("dropping views")
        views.dropViews()

    logger.info("creating views %s..." % datetime.now().isoformat())
    views.makeAllViews()
    views.makeUniqueIndexes()

    logger.info("creating indexes %s ..." % datetime.now().isoformat())
    indexes = SunshineIndexes(connection)
    indexes.makeAllIndexes()

    connection.close()

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Download and import campaign disclosure data from the IL State Board of Elections.')
    parser.add_argument(
        '--download',
        action='store_true',
        help='Downloading fresh data'
    )

    parser.add_argument(
        '--load_data',
        action='store_true',
        help='Load data into database'
    )

    parser.add_argument(
        '--recreate_views',
        action='store_true',
        help='Recreate database views'
    )

    parser.add_argument(
        '--chunk_size',
        help='Adjust the size of each insert when loading data',
        type=int
    )

    main(parser.parse_args())
