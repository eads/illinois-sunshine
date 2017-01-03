from sunshine.models import Committee, Candidate, Officer, Candidacy, \
    D2Report, FiledDoc, Receipt, Expenditure, Investment
import os
from datetime import date, datetime, timedelta
from hashlib import md5
import sqlalchemy as sa
import csv
from csvkit.cleanup import RowChecker
from csvkit.sql import make_table, make_create_table_statement
from csvkit.table import Table
from collections import OrderedDict
from typeinferer import TypeInferer
import psycopg2
from psycopg2.extensions import AsIs
from sunshine.database import db_session

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
        
        self.file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 
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
        
        sql_table = sa.Table(self.table_name, sa.MetaData(), 
                             autoload=True, autoload_with=self.connection.engine)

        if not 'search_name' in sql_table.columns.keys():

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
        sql_table = sa.Table(self.table_name, sa.MetaData(), 
                             autoload=True, autoload_with=self.connection.engine)

        if not 'search_date' in sql_table.columns.keys():
            
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
        
        self.executeTransaction(enum)
        
        self.metadata.create_all(bind=self.connection.engine)
        

    def makeRawTable(self):
        inferer = TypeInferer(self.file_path)
        inferer.infer()
        
        sql_table = sa.Table('raw_{0}'.format(self.table_name), 
                             sa.MetaData())

        for column_name, column_type in inferer.types.items():
            sql_table.append_column(sa.Column(column_name, column_type()))
        
        dialect = sa.dialects.postgresql.dialect()
        create_table = str(sa.schema.CreateTable(sql_table)\
                           .compile(dialect=dialect)).strip(';')

        self.executeTransaction('DROP TABLE IF EXISTS raw_{0}'.format(self.table_name))
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

        self.executeTransaction('DROP TABLE IF EXISTS new_{0}'.format(self.table_name))
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

        fields = ','.join(['{0}=s."{1}"'.format(clean, raw) \
                for clean, raw in zip(self.header, self.raw_header)])
        
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
    
    def addNameColumn(self):
        
        sql_table = sa.Table(self.table_name, sa.MetaData(), 
                             autoload=True, autoload_with=self.connection.engine)

        if not 'search_name' in sql_table.columns.keys():
            
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
    header = [f for f in Candidate.__table__.columns.keys() \
              if f not in ['date_added', 'last_update', 'ocd_id']]
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

        fields = ','.join(['{0}=s."{1}"'.format(clean, raw) \
                for clean, raw in zip(header, self.raw_header)])
        
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

        fields = ','.join(['{0}=s."{1}"'.format(clean, raw) \
                for clean, raw in zip(header, self.raw_header)])
        
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
            row['ElectionType'] = self.election_types.get(row['ElectionType'].strip())
            
            # Get race type
            if row.get('IncChallOpen'):
                row['IncChallOpen'] = self.race_types.get(row['IncChallOpen'].strip())
            
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

        self.executeTransaction('DROP TABLE IF EXISTS new_{0}'.format(self.table_name))
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

        self.executeTransaction('DROP TABLE IF EXISTS new_{0}'.format(self.table_name))
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
            trans.commit()
            return rows
        except (sa.exc.ProgrammingError, psycopg2.ProgrammingError) as e:
            # TODO: this line seems to break when creating views for the first time.
            # logger.error(e, exc_info=True)
            trans.rollback()
            raise e

    def executeOutsideTransaction(self, query):
        
        self.connection.connection.set_isolation_level(0)
        curs = self.connection.connection.cursor()

        try:
            curs.execute(query)
        except psycopg2.ProgrammingError:
            pass

    def dropViews(self):
        self.executeTransaction('DROP MATERIALIZED VIEW IF EXISTS receipts_by_month')
        self.executeTransaction('DROP MATERIALIZED VIEW IF EXISTS committee_receipts_by_week')
        self.executeTransaction('DROP MATERIALIZED VIEW IF EXISTS incumbent_candidates')
        self.executeTransaction('DROP MATERIALIZED VIEW IF EXISTS most_recent_filings CASCADE')
        self.executeTransaction('DROP MATERIALIZED VIEW IF EXISTS expenditures_by_candidate')
        self.executeTransaction('DROP TABLE IF EXISTS contested_races')

    def makeAllViews(self):
        self.incumbentCandidates()
        self.mostRecentFilings()
        self.condensedReceipts()
        self.condensedExpenditures()
        self.expendituresByCandidate() # relies on condensed_expenditures
        self.receiptsAggregates() # relies on condensedReceipts
        self.committeeReceiptAggregates() # relies on condensedReceipts
        self.committeeMoney() # relies on mostRecentFilings
        self.candidateMoney() # relies on committeeMoney and mostRecentFilings
        self.contestedRaces() # relies on sunshine/contested_races_2016.csv and sunshine/comptroller_contested_race_2016.csv
 
    def condensedExpenditures(self):
        
        try:
            
            self.executeTransaction('REFRESH MATERIALIZED VIEW CONCURRENTLY condensed_expenditures')
        
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
            self.executeTransaction('REFRESH MATERIALIZED VIEW CONCURRENTLY condensed_receipts')
        
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
            self.executeTransaction('REFRESH MATERIALIZED VIEW CONCURRENTLY expenditures_by_candidate')
        
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

    def contestedRaces(self):
        """
        Creates the contested races view table from csv files hard saved in sunsine folder
        """    
        try:
            comptroller_input_file = csv.DictReader(open(os.getcwd()+'/sunshine/comptroller_contested_race_2016.csv'))
            races_input_file = csv.DictReader(open(os.getcwd()+'/sunshine/contested_races_2016.csv'))

            entries = []
            for row in races_input_file:
                entries.append(row)
            
            for row in comptroller_input_file:
                entries.append(row)

            contested_races = []
            counter = 0
            for e in entries:
                supporting_funds = 0
                opposing_funds = 0
                controlled_amount = 0
                funds_available  = 0
                contributions = 0
                total_funds = 0
                investments = 0
                debts = 0
                total_money = 0
                candidate_names = [] #list of all possible candidate name possibilities

                try:
                    candidate_id = int(float(e['Candidate ID']))
                except:
                    candidate_id = None

                try:
                    committee_id = int(float(e['ID']))
                except:
                    committee_id = None

                try:
                    district = int(float(e['District']))
                except:
                    district = None

                first_names = e['First'].split(';')
                last_names = e['Last'].split(';')

                first_name = first_names[0].strip()
                last_name = last_names[0].strip()
 
                if candidate_id:
                    first_name, last_name = self.get_candidate_name(candidate_id)
                    if first_name and last_name:
                        candidate_names.append(first_name + " " + last_name)
                       
                for fn in first_names:
                    for ln in last_names:
                        candidate_names.append(fn.strip() + " " + ln.strip())
                        candidate_names.append(ln.strip() + " " + fn.strip())
                        candidate_names.append(ln.strip())

                cand_names = set(candidate_names)

                for cn in cand_names:
                    supp_funds, opp_funds = self.get_candidate_funds_byname(cn)
                    supporting_funds = supporting_funds + supp_funds
                    opposing_funds = opposing_funds + opp_funds

 
                if committee_id:
                    committee, recent_receipts, recent_total, latest_filing, controlled_amount, ending_funds, investments, debts, expenditures, total_expenditures = self.get_committee_details(committee_id)

                    funds_available = latest_filing['end_funds_available']
                    contributions = recent_total
                    total_funds = controlled_amount
                    investments = latest_filing['total_investments']
                    debts = latest_filing['total_debts']
                        
                
                total_money = supporting_funds + opposing_funds + controlled_amount 
                contested_races.append({'district': district, 'branch': e['Senate/House'], 'last_name': last_name, 'first_name': first_name,'committee_name': e['Committee'],'incumbent': e['Incumbent'],'committee_id': committee_id,'party': e['Party'], 'funds_available': funds_available, 'contributions': contributions, 'total_funds': total_funds, 'investments': investments, 'debts': debts, 'supporting_funds': supporting_funds, 'opposing_funds': opposing_funds, 'candidate_id' : candidate_id, 'total_money': total_money, 'reporting_period_end' : latest_filing['reporting_period_end'], 'alternate_names' : ';'.join(cand_names)})

            exp = '''
                CREATE TABLE contested_races(
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
            
            trans = self.connection.begin()	
            curs = self.connection.connection.cursor()
            curs.execute(exp)   
            insert_statement = 'INSERT INTO contested_races (%s) VALUES %s'
            cols = ['last_name', 'committee_id', 'incumbent', 'district', 'first_name', 'total_funds', 'candidate_id', 'investments', 'committee_name', 'supporting_funds', 'opposing_funds', 'party', 'branch', 'contributions', 'debts', 'total_money', 'funds_available','reporting_period_end','alternate_names']
            for cr in contested_races:

                values = [cr[column] for column in cols]
                curs.execute(insert_statement, (AsIs(','.join(cols)), tuple(values)))
                
            trans.commit()
        except sa.exc.ProgrammingError:
            print('Problem in creating contested_races table')

    def get_candidate_name(self,candidate_id):
    
        try:
            candidate_id = int(candidate_id)
        except ValueError:
            return 
  
        cand_sql = '''(
            SELECT *
            FROM candidates 
            WHERE id = :candidate_id
            )
        '''
        candidate = self.executeTransaction(sa.text(cand_sql),candidate_id=candidate_id).fetchone()
        #candidate = db_session.query(Candidate).get(candidate_id)

        if not candidate:
            return 
        else:
            return candidate.first_name, candidate.last_name
              
    def get_candidate_funds_byname(self,candidate_name):
    
        d2_part = '9B'
        expended_date = datetime(2016, 3, 16, 0, 0)

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
    
        supporting_funds = self.executeTransaction(sa.text(supporting_funds_sql), candidate_name=candidate_name,d2_part=d2_part,expended_date=expended_date).fetchone().amount

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
    
        opposing_funds = self.executeTransaction(sa.text(opposing_funds_sql), candidate_name=candidate_name,d2_part=d2_part,expended_date=expended_date).fetchone().amount


        return supporting_funds, opposing_funds


    def get_committee_details(self,committee_id):

        try:
            committee_id = int(committee_id)
        except ValueError:
            return 

        comm_sql = '''(
            SELECT *
            FROM committees
            WHERE id = :committee_id
            )
        '''
        committee = self.executeTransaction(sa.text(comm_sql),committee_id=committee_id).fetchone()
         
        if not committee:
            return 
        
        latest_filing = '''( 
            SELECT * FROM most_recent_filings
            WHERE committee_id = :committee_id
            ORDER BY received_datetime DESC
            LIMIT 1
            )
        '''

        latest_filing = dict(self.executeTransaction(sa.text(latest_filing), 
                                       committee_id=committee_id).fetchone())
        
        params = {'committee_id': committee_id}

        if not latest_filing['reporting_period_end']:
            latest_filing['reporting_period_end'] = datetime.now().date() - timedelta(days=90)

        if latest_filing['end_funds_available'] \
            or latest_filing['end_funds_available'] == 0:

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
            controlled_amount = latest_filing['end_funds_available'] 
            
            params['end_date'] = latest_filing['reporting_period_end']
            end_date = latest_filing['reporting_period_end']

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

        recent_total = self.executeTransaction(sa.text(recent_receipts),**params).fetchone().amount
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

        quarterlies = self.executeTransaction(sa.text(quarterlies), 
                                     committee_id=committee_id)

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

        expenditures = [[r.total_expenditures, 
                         r.reporting_period_end.year,
                         r.reporting_period_end.month,
                         r.reporting_period_end.day] 
                         for r in quarterlies]
        
        #accomodate for independent expenditures past last filing date
        
        total_expenditures = sum([r.total_expenditures for r in quarterlies])

        return committee, recent_receipts, recent_total, latest_filing, controlled_amount, ending_funds, investments, debts, expenditures, total_expenditures


    def receiptsAggregates(self):

        try:
            
            self.executeTransaction('REFRESH MATERIALIZED VIEW CONCURRENTLY receipts_by_month')
        
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
            self.executeTransaction('REFRESH MATERIALIZED VIEW CONCURRENTLY committee_receipts_by_week')

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
            
            self.executeTransaction('REFRESH MATERIALIZED VIEW CONCURRENTLY incumbent_candidates')

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
            
            self.executeTransaction('REFRESH MATERIALIZED VIEW CONCURRENTLY most_recent_filings')
        
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
            
            self.executeTransaction('REFRESH MATERIALIZED VIEW CONCURRENTLY committee_money')
        
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

            self.executeTransaction('REFRESH MATERIALIZED VIEW CONCURRENTLY candidate_money')

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
        Need a unique index on materialized views so that can be refreshed concurrently
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
    

class SunshineIndexes(object):
    def __init__(self, connection):
        self.connection = connection
    
    def executeTransaction(self, query):
        trans = self.connection.begin()

        try:
            self.connection.execute(query)
            trans.commit()
        except sa.exc.ProgrammingError as e:
            trans.rollback()

    def executeOutsideTransaction(self, query):
        
        self.connection.connection.set_isolation_level(0)
        curs = self.connection.connection.cursor()

        try:
            curs.execute(query)
        except psycopg2.ProgrammingError:
            pass

    def makeAllIndexes(self):
        self.receiptsDate()
        self.receiptsCommittee()
        self.receiptsFiledDocs()
        self.candidaciesCandidate()
        self.candidateCommittees()
        self.officersCommittee()
        self.filedDocsCommittee()
        self.receiptsName()
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
    download_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 
                                                     'downloads'))

    download_location = os.path.join(download_path, latest_filename)

    download_url = 'http://il-elections.s3.amazonaws.com/%s' % latest_filename

    filename, _ = urllib.request.urlretrieve(download_url,
                                             filename=download_location)
    
    with zipfile.ZipFile(filename, 'r') as zf:
        date_prefix = zf.namelist()[0].split('/')[0]
        zf.extractall(path=download_path)
    
    #for member in os.listdir(os.path.join(download_path, date_prefix)):
    #    move_from = os.path.join(download_path, date_prefix, member)
    #    move_to = os.path.join(download_path, member)
    #    os.rename(move_from, move_to)

def alterSearchDictionary():
    from sunshine.app_config import DB_HOST, DB_PORT, DB_NAME, STOP_WORD_LIST
    
    alter = ''' 
        ALTER TEXT SEARCH DICTIONARY english_stem (StopWords = '{0}');
    '''.format(STOP_WORD_LIST)
    
    DB_USER = 'postgres'
    DB_PW = ''

    DB_CONN='postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'\
            .format(DB_USER, DB_PW, DB_HOST, DB_PORT, DB_NAME)
    
    engine = sa.create_engine(DB_CONN, 
                              convert_unicode=True, 
                              server_side_cursors=True)
        
    with engine.begin() as conn:
        conn.execute(alter)

if __name__ == "__main__":
    import sys
    import argparse
    from sunshine.app_config import STOP_WORD_LIST
    from sunshine.database import engine, Base

    parser = argparse.ArgumentParser(description='Download and import campaign disclosure data from the IL State Board of Elections.')
    parser.add_argument('--download', action='store_true',
                   help='Downloading fresh data')

    parser.add_argument('--load_data', action='store_true',
                   help='Load data into database')

    parser.add_argument('--recreate_views', action='store_true',
                   help='Recreate database views')
    
    parser.add_argument('--chunk_size', help='Adjust the size of each insert when loading data',
                   type=int)

    args = parser.parse_args()

    connection = engine.connect()

    if args.download:
        logger.info("download start %s ..." % datetime.now().isoformat())
        
        downloadUnzip()

        logger.info("download finish %s ..." % datetime.now().isoformat())
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
        committees.load()
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

        prev_off = SunshinePrevOfficers(connection, chunk_size=chunk_size)
        prev_off.load(update_existing=True)
        
        del prev_off

        candidacy = SunshineCandidacy(connection, chunk_size=chunk_size)
        candidacy.load()
        
        del candidacy

        can_cmte_xwalk = SunshineCandidateCommittees(connection, chunk_size=chunk_size)
        can_cmte_xwalk.load()
        
        del can_cmte_xwalk

        off_cmte_xwalk = SunshineOfficerCommittees(connection, chunk_size=chunk_size)
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
        
        # delete specific rows from receipts table since sboe doesn't remove wrong data from db
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
