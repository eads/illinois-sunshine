from sunshine.database import Base, engine
from sunshine.models import Committee
from sunshine import app_config 
import ftplib
from io import BytesIO
import os
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from datetime import date
from hashlib import md5
import sqlalchemy as sa
import csv

def cacheOnS3():
    keys = []
    with ftplib.FTP(app_config.FTP_HOST) as ftp:
        ftp.login(app_config.FTP_USER, app_config.FTP_PW)
        files = ftp.nlst(app_config.FTP_START)
        for f in files:
            print('working on %s' % f)
            fobj = BytesIO()
            fpath ='%s/%s' % (app_config.FTP_START, f)
            ftp.retrbinary('RETR %s' % fpath, fobj.write)
            
            fobj.seek(0)

            conn = S3Connection(app_config.AWS_KEY, app_config.AWS_SECRET)
            bucket = conn.get_bucket('il-elections')
            
            k = Key(bucket)
            keyname = 'sunshine/%s_%s.%s' % (f, date.today().isoformat(), f.rsplit('.', 1)[-1])
            k.key = keyname
            k.set_contents_from_file(fobj)
            k.make_public()
            
            keys.append(keyname)
            
            bucket.copy_key('sunshine/%s_latest.txt' % (f), 
                            'il-elections',
                            keyname,
                            preserve_acl=True)

    return keys

def transformCommittees(fpath):
    with open(fpath, 'r', encoding='latin1') as f:
        reader = csv.reader(f, delimiter='\t')
        header = next(reader)
        real_header = Committee.__table__.columns.keys()
        rows = []
        for row in reader:
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
            
            rows.append(dict(zip(real_header, row)))
            if len(rows) % 10000 is 0:
                insert = Committee.__table__.insert()
                with engine.begin() as conn:
                    conn.execute(insert, rows)
                rows = []


    return fpath

def transformCandidates(fpath):
    return fpath

def transformOfficers(fpath):
    return fpath

def transformElections(fpath):
    return fpath

TRANSFORMER = {
    'Committees': transformCommittees,
    'Candidates': transformCandidates,
    'CanElections': transformElections,
    'Officers': transformOfficers,
    'PrevOfficers': transformOfficers,
}

def createPositionENUM():
    enum = ''' 
        CREATE TYPE committee_position AS ENUM (
          'support', 
          'oppose'
        )
    '''
    conn = engine.connect()
    trans = conn.begin()
    
    try:
        conn.execute(enum)
        trans.commit()
    except sa.exc.ProgrammingError:
        trans.rollback()

def initializeDB():
    Base.metadata.create_all(bind=engine)
    for fpath, _ in getLatestFiles():
        try:
            k = fpath.split('.', 1)[0].replace('downloads/', '')
            TRANSFORMER[k](fpath)
        except KeyError as e:
            pass
        except IndexError as e:
            print(e, 'indexerror')

def getLatestFiles():
    latest_files = []
    conn = S3Connection(app_config.AWS_KEY, app_config.AWS_SECRET)
    bucket = conn.get_bucket('il-elections')
    for key in bucket.list(prefix='sunshine'):
        if key.name.endswith('latest.txt'):
            fpath = 'downloads/%s' % key.name.replace('sunshine/', '')
            latest_files.append((fpath, key.etag))
    return latest_files

def updateDB():
    for fpath, etag in getLatestFiles():
        print(etag)

if __name__ == "__main__":
    import sys
    
    try:
        if sys.argv[1] == 'cache':
            cacheOnS3()
        elif sys.argv[1] == 'initialize':
            initializeDB()
        elif sys.argv[1] == 'update':
            updateDB()
    except IndexError:
        print('Need an argument')
        sys.exit()
