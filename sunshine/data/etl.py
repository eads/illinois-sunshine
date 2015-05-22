from sunshine.database import engine
from sunshine import app_config 
import ftplib
from io import BytesIO
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from datetime import date

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
            keyname = '%s_%s.%s' % (f, date.today().isoformat(), f.rsplit('.', 1)[-1])
            k.key = keyname
            k.set_contents_from_file(fobj)
            
            keys.append(keyname)
    
    return keys
