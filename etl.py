from sunshine.models import Committee, Candidate, Officer, Candidacy, \
    D2Report, FiledDoc, Receipt, Expenditure, Investment
import ftplib
from io import BytesIO
import os
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from datetime import date
from hashlib import md5
import sqlalchemy as sa
import csv
from csvkit.cleanup import RowChecker
from collections import OrderedDict

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

class SunshineTransformLoad(object):

    def __init__(self, 
                 engine,
                 metadata,
                 chunk_size=50000):

        
        self.engine = engine
        self.metadata = metadata
        
        self.chunk_size = chunk_size

        self.initializeDB()

        
        self.file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 
                                      'downloads', 
                                      self.filename)

    def initializeDB(self):
        enum = ''' 
            CREATE TYPE committee_position AS ENUM (
              'support', 
              'oppose'
            )
        '''
        conn = self.engine.connect()
        trans = conn.begin()
        
        try:
            conn.execute(enum)
            trans.commit()
        except sa.exc.ProgrammingError:
            trans.rollback()
        
        self.metadata.create_all(bind=self.engine)
    
    def createTempTable(self):
        create = ''' 
            CREATE TABLE temp_{0} AS
              SELECT * FROM {0} LIMIT 1
            WITH NO DATA
        '''.format(self.table_name)
        with self.engine.begin() as conn:
            conn.execute('DROP TABLE IF EXISTS temp_{0}'.format(self.table_name))
            conn.execute(create)
    
    @property
    def upsert(self):
        field_format = '{1} = subq.{1}'
        
        update_fields = [field_format.format(self.table_name,f) \
                             for f in self.header]
        
        return ''' 
            WITH upsert AS (
              UPDATE {0} SET 
                {1}
              FROM (
                SELECT * FROM temp_{0}
              ) AS subq
              WHERE {0}.id = subq.id
              RETURNING *
            )
            INSERT INTO {0} 
              SELECT * FROM temp_{0}
            WHERE NOT EXISTS (SELECT * FROM upsert)
        '''.format(self.table_name, 
                   ','.join(update_fields))

    def update(self):

        with self.engine.begin() as conn:
            conn.execute(sa.text(self.upsert))

        with self.engine.begin() as conn:
            conn.execute('DROP TABLE temp_{0}'.format(self.table_name))

    def transform(self):
        with open(self.file_path, 'r', encoding='latin1') as f:
            reader = csv.reader(f, delimiter='\t', 
                                quoting=csv.QUOTE_NONE)
            checker = RowChecker(reader)
            for row in checker.checked_rows():
                if row:
                    for idx, cell in enumerate(row):
                        row[idx] = cell.strip()
                        if not row[idx]:
                            row[idx] = None
                    yield OrderedDict(zip(self.header, row))

    def load(self):
        self.createTempTable()
        
        insert = ''' 
            INSERT INTO temp_{0} ({1}) VALUES ({2})
        '''.format(self.table_name,
                   ','.join(self.header),
                   ','.join([':%s' % h for h in self.header]))

        rows = []
        i = 1
        for row in self.transform():
            rows.append(row)
            if len(rows) % self.chunk_size is 0:
                
                with self.engine.begin() as conn:
                    conn.execute(sa.text(insert), *rows)
                
                print('Loaded %s %s' % ((i * self.chunk_size), self.table_name))
                i += 1
                rows = []
        if rows:
            with self.engine.begin() as conn:
                conn.execute(sa.text(insert), *rows)
    
class SunshineCommittees(SunshineTransformLoad):
    
    table_name = 'committees'
    header = Committee.__table__.columns.keys()
    filename = 'Committees.txt'
    
    def transform(self):
        with open(self.file_path, 'r', encoding='latin1') as f:
            reader = csv.reader(f, delimiter='\t')
            header = next(reader)
            for row in reader:
                if row:
                    for idx, cell in enumerate(row):
                        row[idx] = cell.strip()
                        if not cell:
                            row[idx] = None

                    # Replace status value
                    if row[14] != 'A':
                        row[14] = False
                    else:
                        row[14] = True

                    # Replace position values
                    for idx in [23, 24]:
                        if row[idx] == 'O':
                            row[idx] = 'oppose'
                        elif row[idx] == 'S':
                            row[idx] = 'support'
                        else:
                            row[idx] = None
                    
                    yield OrderedDict(zip(self.header, row))
    

class SunshineCandidates(SunshineTransformLoad):
    
    table_name = 'candidates'
    header = [f for f in Candidate.__table__.columns.keys() \
              if f not in ['date_added', 'last_update', 'ocd_id']]
    filename = 'Candidates.txt'
    
    @property
    def upsert(self):
        field_format = '{1} = subq.{1}'
        
        update_fields = [field_format.format(self.table_name,f) \
                             for f in self.header]
        
        return ''' 
            WITH upsert AS (
              UPDATE {0} SET 
                {1},
                last_update = NOW()
              FROM (
                SELECT * FROM temp_{0}
              ) AS subq
              WHERE {0}.id = subq.id
              RETURNING *
            )
            INSERT INTO {0} ({2})
              SELECT 
                {3},
                NOW() AS last_update,
                NOW() AS date_added
              FROM temp_{0}
            WHERE NOT EXISTS (SELECT * FROM upsert)
        '''.format(self.table_name, 
                   ','.join(update_fields),
                   ','.join(self.header + ['last_update', 'date_added']),
                   ','.join(self.header))

class SunshineOfficers(SunshineTransformLoad):
    table_name = 'officers'
    header = Officer.__table__.columns.keys()
    filename = 'Officers.txt'
    current = True

    def transform(self):
        with open(self.file_path, 'r', encoding='latin1') as f:
            reader = csv.reader(f, delimiter='\t')
            header = next(reader)
            for row in reader:
                if row:
                    
                    for idx, cell in enumerate(row):
                        row[idx] = cell.strip()
                        
                        if not cell:
                            row[idx] = None
                    
                    # Add empty committee_id
                    row.insert(1, None)

                    # Add empty resign date
                    row.insert(11, None)

                    # Add current flag
                    row.append(self.current)
                    
                    yield OrderedDict(zip(self.header, row))
    
class SunshinePrevOfficers(SunshineTransformLoad):
    table_name = 'officers'
    header = Officer.__table__.columns.keys()
    filename = 'PrevOfficers.txt'
    current = False
    
    def transform(self):
        with open(self.file_path, 'r', encoding='latin1') as f:
            reader = csv.reader(f, delimiter='\t')
            header = next(reader)
            for row in reader:
                if row:
                    for idx, cell in enumerate(row):
                        row[idx] = cell.strip()
                        if not cell:
                            row[idx] = None
                    
                    # Add empty phone
                    row.insert(10, None)

                    # Add current flag
                    row.append(self.current)

                    yield OrderedDict(zip(self.header, row))

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
        with open(self.file_path, 'r', encoding='latin1') as f:
            reader = csv.reader(f, delimiter='\t')
            header = next(reader)
            for row in reader:
                if row:
                    for idx, cell in enumerate(row):
                        row[idx] = cell.strip()
                        if not cell:
                            row[idx] = None

                    # Get election type
                    row[2] = self.election_types.get(row[2])
                    
                    # Get race type
                    row[4] = self.race_types.get(row[4])
                    
                    # Get outcome
                    if row[5] == 'Won':
                        row[5] = 'won'
                    elif row[5] == 'Lost':
                        row[5] = 'lost'
                    else:
                        row[5] = None

                    yield OrderedDict(zip(self.header, row))


class SunshineCandidateCommittees(SunshineTransformLoad):
    table_name = 'candidate_committees'
    header = ['committee_id', 'candidate_id']
    filename = 'CmteCandidateLinks.txt'
    
    def transform(self):
        with open(self.file_path, 'r', encoding='latin1') as f:
            reader = csv.reader(f, delimiter='\t')
            header = next(reader)
            for row in reader:
                if row:
                    for idx, cell in enumerate(row):
                        row[idx] = cell.strip()
                        if not cell:
                            row[idx] = None
                    row.pop(0)
                    yield OrderedDict(zip(self.header, row))

    @property
    def upsert(self):
        field_format = '{1} = subq.{1}'
        
        update_fields = [field_format.format(self.table_name,f) \
                             for f in self.header]
        
        where_clause = ''' 
            WHERE {0}.{1} = subq.{1}
              AND {0}.{2} = subq.{2}
        '''.format(self.table_name, 
                   self.header[0], 
                   self.header[1])

        return ''' 
            WITH upsert AS (
              UPDATE {0} SET 
                {1}
              FROM (
                SELECT * FROM temp_{0}
              ) AS subq
              {2}
              RETURNING *
            )
            INSERT INTO {0} 
              SELECT * FROM temp_{0}
            WHERE NOT EXISTS (SELECT * FROM upsert)
        '''.format(self.table_name, 
                   ','.join(update_fields),
                   where_clause)

class SunshineOfficerCommittees(SunshineTransformLoad):
    table_name = 'officers'
    header = ['committee_id', 'officer_id']
    filename = 'CmteOfficerLinks.txt'
    
    def transform(self):
        with open(self.file_path, 'r', encoding='latin1') as f:
            reader = csv.reader(f, delimiter='\t')
            header = next(reader)
            for row in reader:
                if row:
                    for idx, cell in enumerate(row):
                        row[idx] = cell.strip()
                        if not cell:
                            row[idx] = None
                    row.pop(0)
                    yield OrderedDict(zip(self.header, row))

    def createTempTable(self):
        create = ''' 
            CREATE TABLE temp_{0} (
              committee_id INTEGER, 
              officer_id INTEGER
            )
        '''.format(self.table_name)
        with self.engine.begin() as conn:
            conn.execute('DROP TABLE IF EXISTS temp_{0}'.format(self.table_name))
            conn.execute(create)
    
    @property
    def upsert(self):

        return ''' 
              UPDATE officers SET 
                committee_id = subq.committee_id
              FROM (
                SELECT * FROM temp_{0}
              ) AS subq
              WHERE officers.id = subq.officer_id
        '''.format(self.table_name)

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
    
    def __init__(self, engine):
        self.engine = engine

    def makeAllViews(self):
        self.incumbentMoney()
        self.committeeMoney()
        self.namesView()

    def committeeMoney(self):
        conn = self.engine.connect()
        trans = conn.begin()
        try:
            conn.execute('REFRESH MATERIALIZED VIEW all_quarterly_filings')
            trans.commit()
        except sa.exc.ProgrammingError:
            trans.rollback()
            conn = self.engine.connect()
            trans = conn.begin()
            create = '''
               CREATE MATERIALIZED VIEW all_quarterly_filings AS (
                 SELECT * FROM (
                   SELECT DISTINCT ON (doc.doc_name, committee.id, committee.candidate_id)
                     d2.end_funds_available,
                     committee.id AS committee_id,
                     committee.name AS committee_name,
                     doc.received_datetime,
                     doc.reporting_period_end,
                     doc.reporting_period_begin,
                     doc.id AS filed_doc_id,
                     committee.candidate_id, 
                     committee.candidate_last_name,
                     committee.candidate_first_name,
                     committee.candidate_office,
                     committee.candidate_district
                   FROM d2_reports AS d2
                   JOIN (
                     SELECT 
                       cm.id,
                       cm.name,
                       cand.id AS candidate_id,
                       cand.first_name AS candidate_first_name,
                       cand.last_name AS candidate_last_name,
                       cand.office AS candidate_office,
                       cand.district AS candidate_district
                     FROM committees AS cm
                     JOIN candidate_committees AS cc
                       ON cm.id = cc.committee_id
                     JOIN candidates AS cand
                       ON cc.candidate_id = cand.id
                   ) AS committee
                     ON d2.committee_id = committee.id
                   JOIN filed_docs AS doc
                     ON d2.filed_doc_id = doc.id
                   WHERE doc.doc_name = :doc_name
                   ORDER BY doc.doc_name, 
                            committee.id,
                            committee.candidate_id,
                            doc.received_datetime DESC
                 ) AS rows 
                 ORDER BY end_funds_available DESC
               )
            '''
            conn.execute(sa.text(create), doc_name='Quarterly')
            trans.commit()

    def incumbentMoney(self, 
                       outcome='won', 
                       election_year=2014,
                       committee_type='Candidate',
                       doc_name='Quarterly'):
        
        conn = self.engine.connect()
        trans = conn.begin()
        try:
            conn.execute('REFRESH MATERIALIZED VIEW incumbent_quarterly_filings')
            trans.commit()
        except sa.exc.ProgrammingError:
            trans.rollback()
            conn = self.engine.connect()
            trans = conn.begin()
            create = '''
               CREATE MATERIALIZED VIEW incumbent_quarterly_filings AS (
                 SELECT * FROM (
                   SELECT DISTINCT ON (doc.doc_name, committee.id, committee.candidate_id)
                     d2.end_funds_available,
                     committee.id AS committee_id,
                     committee.name AS committee_name,
                     doc.received_datetime,
                     doc.reporting_period_end,
                     doc.reporting_period_begin,
                     committee.candidate_id, 
                     committee.candidate_last_name,
                     committee.candidate_first_name,
                     committee.candidate_office,
                     committee.candidate_district
                   FROM d2_reports AS d2
                   JOIN (
                     SELECT 
                       cm.id,
                       cm.name,
                       cand.id AS candidate_id,
                       cand.first_name AS candidate_first_name,
                       cand.last_name AS candidate_last_name,
                       cand.office AS candidate_office,
                       cand.district AS candidate_district
                     FROM committees AS cm
                     JOIN candidate_committees AS cc
                       ON cm.id = cc.committee_id
                     JOIN (
                       SELECT DISTINCT ON (cd.district, cd.office)
                         cd.*
                       FROM candidates AS cd
                       JOIN candidacies AS cs
                         ON cd.id = cs.candidate_id
                       WHERE cs.outcome = :outcome
                         AND cs.election_year >= :year
                       ORDER BY cd.district, cd.office, cs.id DESC
                     ) AS cand
                       ON cc.candidate_id = cand.id
                     WHERE cm.type = :committee_type
                   ) AS committee
                     ON d2.committee_id = committee.id
                   JOIN filed_docs AS doc
                     ON d2.filed_doc_id = doc.id
                   WHERE doc.doc_name = :doc_name
                   ORDER BY doc.doc_name, 
                            committee.id,
                            committee.candidate_id,
                            doc.received_datetime DESC
                 ) AS rows 
                 ORDER BY end_funds_available DESC
               )
            '''
            params = {
                'doc_name': doc_name,
                'year': election_year,
                'outcome': outcome,
                'committee_type': committee_type,
            }
            conn.execute(sa.text(create), **params)
            trans.commit()
    
    def namesView(self):
        conn = self.engine.connect()
        trans = conn.begin()
        try:
            conn.execute('REFRESH MATERIALIZED VIEW all_names')
            trans.commit()
        except sa.exc.ProgrammingError:
            trans.rollback()
            conn = self.engine.connect()
            trans = conn.begin()
            
            # This should aggregate by name and 
            # return the tables that name appears in with the ids

            create = ''' 
                CREATE MATERIALIZED VIEW all_names AS (
                    SELECT 
                      array_agg(table_id) AS table_ids, 
                      TRIM(name) AS name, 
                      table_name
                    FROM (
                        SELECT 
                          id AS table_id,
                          COALESCE(TRIM(TRANSLATE(first_name, '.,-/', '')), '') || ' ' ||
                          COALESCE(TRIM(TRANSLATE(last_name, '.,-/', '')), '') AS name,
                          'candidates' AS table_name
                        FROM candidates
                        UNION ALL
                          SELECT
                            id AS table_id,
                            COALESCE(TRIM(TRANSLATE(name, '.,-/', '')), '') AS name,
                            'committees' AS table_name
                          FROM committees
                        UNION ALL
                          SELECT
                            id AS table_id,
                            COALESCE(TRIM(TRANSLATE(first_name, '.,-/', '')), '') || ' ' ||
                            COALESCE(TRIM(TRANSLATE(last_name, '.,-/', '')), '') AS name,
                            'receipts' AS table_name
                          FROM receipts
                        UNION ALL
                          SELECT
                            id AS table_id,
                            COALESCE(TRIM(TRANSLATE(first_name, '.,-/', '')), '') || ' ' ||
                            COALESCE(TRIM(TRANSLATE(last_name, '.,-/', '')), '') AS name,
                            'expenditures' AS table_name
                          FROM expenditures
                        UNION ALL
                          SELECT
                            id AS table_id,
                            COALESCE(TRIM(TRANSLATE(first_name, '.,-/', '')), '') || ' ' ||
                            COALESCE(TRIM(TRANSLATE(last_name, '.,-/', '')), '') AS name,
                            'officers' AS table_name
                          FROM officers
                        UNION ALL
                          SELECT
                            id AS table_id,
                            COALESCE(TRIM(TRANSLATE(first_name, '.,-/', '')), '') || ' ' ||
                            COALESCE(TRIM(TRANSLATE(last_name, '.,-/', '')), '') AS name,
                            'investments' AS table_name
                          FROM investments
                    ) AS s
                    GROUP BY name, table_name
                )
            '''
            conn.execute(sa.text(create))
            trans.commit()


class SunshineIndexes(object):
    def __init__(self, engine):
        self.engine = engine

    def makeAllIndexes(self):
        self.receiptsSearch()
        self.nameSearch()

    def nameSearch(self):
        ''' 
        Search names across all tables
        '''
        index = ''' 
            CREATE INDEX name_index ON all_names
            USING gin(to_tsvector('english', name))
        '''
        conn = self.engine.connect()
        trans = conn.begin()
        try:
            conn.execute(index)
            trans.commit()
        except sa.exc.ProgrammingError as e:
            print(e)
            trans.rollback()
            return

    def receiptsSearch(self):
        
        alter = '''
            ALTER TABLE receipts ADD COLUMN search_index tsvector
        '''

        conn = self.engine.connect()
        trans = conn.begin()
        try:
            conn.execute(alter)
            trans.commit()
        except sa.exc.ProgrammingError as e:
            trans.rollback()
            return

        update = ''' 
            UPDATE receipts SET
              search_index = to_tsvector('english', 
                                         COALESCE(last_name, '') || 
                                         ' ' ||
                                         COALESCE(first_name, '') || 
                                         ' ' || 
                                         COALESCE(employer, '') || 
                                         ' ' ||
                                         COALESCE(description, '') ||
                                         ' ' ||
                                         COALESCE(vendor_last_name, '') ||
                                         ' ' ||
                                         COALESCE(vendor_first_name, ''))
        '''

        with self.engine.begin() as conn:
            conn.execute(update)

        index = ''' 
            CREATE INDEX receipts_search_idx ON receipts 
            USING gin(search_index)
        '''
        
        with self.engine.begin() as conn:
            conn.execute(index)

        trigger = ''' 
            CREATE TRIGGER receipts_search_update
            BEFORE INSERT OR UPDATE ON receipts
            FOR EACH ROW EXECUTE PROCEDURE
            tsvector_update_trigger(search_index, 
                                    'pg_catalog.english',
                                    last_name, 
                                    first_name, 
                                    employer, 
                                    description,
                                    vendor_last_name, 
                                    vedor_first_name)
        '''
        
        with self.engine.begin() as conn:
            conn.execute(trigger)

if __name__ == "__main__":
    import sys
    from sunshine import app_config 
    from sunshine.database import engine, Base

    extract = SunshineExtract(ftp_host=app_config.FTP_HOST,
                              ftp_path=app_config.FTP_PATH,
                              ftp_user=app_config.FTP_USER,
                              ftp_pw=app_config.FTP_PW,
                              aws_key=app_config.AWS_KEY,
                              aws_secret=app_config.AWS_SECRET)
    
    extract.download(cache=False)

    committees = SunshineCommittees(engine, Base.metadata)
    committees.load()
    committees.update()
    
    candidates = SunshineCandidates(engine, Base.metadata)
    candidates.load()
    candidates.update()
    
    officers = SunshineOfficers(engine, Base.metadata)
    officers.load()
    officers.update()
    
    prev_off = SunshinePrevOfficers(engine, Base.metadata)
    prev_off.load()
    prev_off.update()
    
    candidacy = SunshineCandidacy(engine, Base.metadata)
    candidacy.load()
    candidacy.update()
    
    can_cmte_xwalk = SunshineCandidateCommittees(engine, Base.metadata)
    can_cmte_xwalk.load()
    can_cmte_xwalk.update()
    
    off_cmte_xwalk = SunshineOfficerCommittees(engine, Base.metadata)
    off_cmte_xwalk.load()
    off_cmte_xwalk.update()
    
    filed_docs = SunshineFiledDocs(engine, Base.metadata)
    filed_docs.load()
    filed_docs.update()
    
    d2_reports = SunshineD2Reports(engine, Base.metadata)
    d2_reports.load()
    d2_reports.update()
    
    receipts = SunshineReceipts(engine, Base.metadata)
    receipts.load()
    receipts.update()
    
    expenditures = SunshineExpenditures(engine, Base.metadata)
    expenditures.load()
    expenditures.update()
    
    investments = SunshineInvestments(engine, Base.metadata)
    investments.load()
    investments.update()

    views = SunshineViews(engine)
    views.makeAllViews()
