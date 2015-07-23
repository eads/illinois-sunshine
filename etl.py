from sunshine.models import Committee, Candidate, Officer, Candidacy, \
    D2Report, FiledDoc, Receipt, Expenditure, Investment
import ftplib
import zipfile
from io import BytesIO, StringIO
import os
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from datetime import date, datetime
from hashlib import md5
import sqlalchemy as sa
import csv
from csvkit.cleanup import RowChecker
from csvkit.sql import make_table, make_create_table_statement
from csvkit.table import Table
from collections import OrderedDict
from typeinferer import TypeInferer
import psycopg2

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


class SunshineExtract(object):
    
    def __init__(self, 
                 download_path='downloads',
                 ftp_host=None,
                 ftp_path=None,
                 ftp_user=None,
                 ftp_pw=None,
                 aws_key=None,
                 aws_secret=None):
        
        self.ftp_host = ftp_host
        self.ftp_user = ftp_user
        self.ftp_pw = ftp_pw
        self.ftp_path = ftp_path

        self.aws_key = aws_key
        self.aws_secret = aws_secret
        
        self.bucket_name = 'il-elections'
        self.download_path = download_path
    
    def downloadRaw(self):
        fpaths = []
        with ftplib.FTP(self.ftp_host) as ftp:
            ftp.login(self.ftp_user, self.ftp_pw)
            files = ftp.nlst(self.ftp_path)
            for f in files:
                print('downloading %s' % f)
                fname, fext = f.rsplit('.', 1)
                
                remote_path ='%s/%s' % (self.ftp_path, f)
                local_path = '%s/%s' % (self.download_path, f)

                with open(local_path, 'wb') as fobj:
                    ftp.retrbinary('RETR %s' % remote_path, fobj.write)
                
                fpaths.append(local_path)
        
        return fpaths

    def cacheOnS3(self, fpath):
        
        fname, fext = fpath.rsplit('/', 1)[1].rsplit('.', 1)
        
        print('caching %s.%s' % (fname, fext))

        conn = S3Connection(self.aws_key, self.aws_secret)
        bucket = conn.get_bucket(self.bucket_name)
        
        k = Key(bucket)
        keyname = 'sunshine/%s_%s.%s' % (fname, 
                                         date.today().isoformat(), 
                                         fext)
        k.key = keyname
        
        with open(fpath, 'rb') as fobj:
            k.set_contents_from_file(fobj)
        
        k.make_public()
        
        bucket.copy_key('sunshine/%s_latest.%s' % (fname, fext), 
                        self.bucket_name,
                        keyname,
                        preserve_acl=True)
    
    def download(self, cache=True):
        fpaths = self.downloadRaw()
        
        if cache:
            for path in fpaths:
                self.cacheOnS3(path)
            
            self.zipper()

    def zipper(self):
        outp = BytesIO()
        now = datetime.now().strftime('%Y-%m-%d')
        zf_name = 'IL_Elections_%s' % now
        with zipfile.ZipFile(outp, mode='w') as zf:
            for f in os.listdir(self.download_path):
                if f.endswith('.txt'):
                    zf.write(os.path.join(self.download_path, f), 
                             '%s/%s' % (zf_name, f),
                             compress_type=zipfile.ZIP_DEFLATED)
        
        conn = S3Connection(self.aws_key, self.aws_secret)
        bucket = conn.get_bucket(self.bucket_name)
        k = Key(bucket)
        k.key = '%s.zip' % zf_name
        outp.seek(0)
        k.set_contents_from_file(outp)
        k.make_public()
        bucket.copy_key(
            'IL_Elections_latest.zip', 
            'il-elections', 
            '%s.zip' % zf_name,
            preserve_acl=True)
        
        del outp

class SunshineTransformLoad(object):

    def __init__(self, 
                 connection,
                 metadata=None,
                 chunk_size=50000):

        
        self.connection = connection

        self.chunk_size = chunk_size

        if metadata:
            self.metadata = metadata
            self.initializeDB()
        
        self.file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 
                                      'downloads', 
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
            print(e)
            trans.rollback()
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
                                                       COALESCE(last_name, ''))
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
            header = next(reader)
            checker = RowChecker(reader)
            
            with open('%s_raw.csv' % self.file_path, 'w') as outp:
                writer = csv.writer(outp)

                writer.writerow(header)
                
                for row in checker.checked_rows():
                    writer.writerow(row)

    def bulkLoadRawData(self):
        import psycopg2
        from sunshine.app_config import DB_USER, DB_PW, DB_HOST, \
            DB_PORT, DB_NAME
        
        DB_CONN_STR = 'host={0} dbname={1} user={2} port={3}'\
            .format(DB_HOST, DB_NAME, DB_USER, DB_PORT)

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

    def load(self):
        self.makeRawTable()
        self.writeRawToDisk()
        self.bulkLoadRawData()
        self.findNewRecords()

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
                  search_name = to_tsvector('english', name)
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
            row['ElectionType'] = self.election_types.get(row['ElectionType'])
            
            # Get race type
            row['IncChallOpen'] = self.race_types.get(row['IncChallOpen'])
            
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
    
    def transform(self):
        for row in self.iterIncomingData():
            row = [row['CommitteeID'], row['OfficerID']]
            yield OrderedDict(zip(self.header, row))
    
    def load(self):
        self.makeRawTable()
        self.writeRawToDisk()
        self.bulkLoadRawData()

        update = ''' 
            UPDATE officers SET
              committee_id = subq."CommitteeID"
            FROM (
              SELECT 
                "OfficerID", 
                "CommitteeID"
              FROM raw_officer_committees
            ) AS subq
            WHERE officers.id = subq."OfficerID"
        '''
        
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
            self.connection.execute(query, **kwargs)
            trans.commit()
        except sa.exc.ProgrammingError as e:
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
        self.executeTransaction('DROP MATERIALIZED VIEW IF EXISTS receipts_by_week')
        self.executeTransaction('DROP MATERIALIZED VIEW IF EXISTS committee_receipts_by_week')
        self.executeTransaction('DROP MATERIALIZED VIEW IF EXISTS incumbent_candidates')
        self.executeTransaction('DROP MATERIALIZED VIEW IF EXISTS most_recent_filings CASCADE')
        self.executeTransaction('DROP MATERIALIZED VIEW IF EXISTS expenditures_by_candidate')

    def makeAllViews(self):
        self.expendituresByCandidate()
        self.incumbentCandidates()
        self.mostRecentFilings()
        self.condensedReceipts()
        self.condensedExpenditures()
        self.receiptsAggregates() # relies on condensedReceipts
        self.committeeReceiptAggregates() # relies on condensedReceipts
        self.committeeMoney() # relies on mostRecentFilings
        self.candidateMoney() # relies on committeeMoney and mostRecentFilings
    
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
                    WHERE e.expended_date > m.reporting_period_end
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
                    JOIN most_recent_filings AS m
                      USING(committee_id)
                    WHERE r.received_date > m.reporting_period_end
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
                    c.id AS candidate_id,
                    MAX(c.first_name) AS first_name,
                    MAX(c.last_name) AS last_name,
                    MAX(c.office) AS office,
                    cm.id AS committee_id,
                    MAX(cm.name) AS committee_name,
                    MAX(cm.type) AS committee_type,
                    bool_or(e.supporting) AS supporting,
                    bool_or(e.opposing) AS opposing,
                    SUM(e.amount) AS total_amount,
                    MIN(e.expended_date) AS min_date,
                    MAX(e.expended_date) AS max_date
                  FROM candidates AS c
                  JOIN expenditures AS e
                    ON c.first_name || ' ' || c.last_name = e.candidate_name
                  JOIN committees AS cm
                    ON e.committee_id = cm.id
                  GROUP BY cm.id, c.id
                )
            '''
            self.executeTransaction(exp)
            
            self.expendituresByCandidateIndex()

    def receiptsAggregates(self):

        try:
            
            self.executeTransaction('REFRESH MATERIALIZED VIEW CONCURRENTLY receipts_by_week')
        
        except sa.exc.ProgrammingError:

            weeks = ''' 
                CREATE MATERIALIZED VIEW receipts_by_week AS (
                  SELECT 
                    date_trunc('week', received_date) AS week,
                    SUM(amount) AS total_amount,
                    COUNT(id) AS donation_count,
                    AVG(amount) AS average_donation
                  FROM condensed_receipts
                  GROUP BY date_trunc('week', received_date)
                  ORDER BY week
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

        except sa.exc.ProgrammingError:
            
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
                   d2.end_funds_available, 
                   d2.total_investments,
                   d2.total_debts,
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
                     'B-1'
                   ) 
                   ORDER BY committee_id, received_datetime DESC
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
                   MAX(filings.committee_name) AS committee_name,
                   MAX(filings.committee_id) AS committee_id,
                   MAX(filings.committee_type) AS committee_type,
                   bool_and(filings.committee_active) AS committee_active,
                   MAX(filings.doc_name) AS doc_name,
                   MAX(filings.reporting_period_end) AS reporting_period_end,
                   MAX(filings.reporting_period_begin) AS reporting_period_begin,
                   (SUM(COALESCE(receipts.amount, 0)) + 
                    MAX(COALESCE(filings.end_funds_available, 0)) + 
                    MAX(COALESCE(filings.total_investments, 0)) - 
                    MAX(COALESCE(filings.total_debts, 0))) AS total,
                   MAX(receipts.received_date) AS last_receipt_date
                 FROM most_recent_filings AS filings
                 LEFT JOIN receipts
                   ON receipts.committee_id = filings.committee_id
                   AND receipts.received_date > filings.reporting_period_end
                 GROUP BY filings.committee_id
                 ORDER BY total DESC NULLS LAST
               )
            '''
            self.executeTransaction(create)
            
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
            ON expenditures_by_candidate(candidate_id, committee_id)
        '''

        self.executeOutsideTransaction(index)

    def receiptsByWeekIndex(self):
        index = ''' 
            CREATE UNIQUE INDEX CONCURRENTLY receipts_by_week_idx 
            ON receipts_by_week(week)
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
            ON most_recent_filings(committee_id)
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


if __name__ == "__main__":
    import sys
    import argparse
    from sunshine import app_config 
    from sunshine.database import engine, Base

    parser = argparse.ArgumentParser(description='Download and import campaign disclosure data from the IL State Board of Elections.')
    parser.add_argument('--download', action='store_true',
                   help='Downloading fresh data')

    parser.add_argument('--cache', action='store_true',
                   help='Cache downloaded files to S3')

    parser.add_argument('--load_data', action='store_true',
                   help='Load data into database')

    parser.add_argument('--recreate_views', action='store_true',
                   help='Recreate database views')
    
    parser.add_argument('--chunk_size', help='Adjust the size of each insert when loading data',
                   type=int)

    args = parser.parse_args()

    extract = SunshineExtract(ftp_host=app_config.FTP_HOST,
                              ftp_path=app_config.FTP_PATH,
                              ftp_user=app_config.FTP_USER,
                              ftp_pw=app_config.FTP_PW,
                              aws_key=app_config.AWS_KEY,
                              aws_secret=app_config.AWS_SECRET)
    
    connection = engine.connect()

    if args.download:
        logger.info("download start %s ..." % datetime.now().isoformat())
        extract.download(cache=args.cache)
        logger.info("download finish %s ..." % datetime.now().isoformat())
    else:
        print("skipping download")
    
    del extract

    if args.load_data:
        print("loading data start %s ..." % datetime.now().isoformat())
        
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
        candidates.load()
        candidates.addNameColumn()
        candidates.addDateColumn('NULL')
        
        del candidates

        officers = SunshineOfficers(connection, chunk_size=chunk_size)
        officers.load()
        officers.addNameColumn()
        officers.addDateColumn('NULL')
        
        del officers

        prev_off = SunshinePrevOfficers(connection, chunk_size=chunk_size)
        prev_off.load()
        
        del prev_off

        candidacy = SunshineCandidacy(connection, chunk_size=chunk_size)
        candidacy.load()
        
        del candidacy

        can_cmte_xwalk = SunshineCandidateCommittees(connection, chunk_size=chunk_size)
        can_cmte_xwalk.load()
        
        del can_cmte_xwalk

        off_cmte_xwalk = SunshineOfficerCommittees(connection, chunk_size=chunk_size)
        off_cmte_xwalk.load()
        
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
