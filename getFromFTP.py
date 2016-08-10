import ftplib
import zipfile
from io import BytesIO, StringIO
import os
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from datetime import date, datetime
from hashlib import md5

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
        
        self.bucket_name = 'illinoissunshine-etl'
        self.download_path = download_path
    
    def downloadRaw(self):
        fpaths = []
        with ftplib.FTP(self.ftp_host) as ftp:
            ftp.login(self.ftp_user, self.ftp_pw)
            print(ftp.dir(self.ftp_path))
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

        conn = S3Connection()
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
        now = datetime.now().strftime('%Y-%m-%d')
        zf_name = 'IL_Campaign_Disclosure_%s' % now
        with zipfile.ZipFile(zf_name, mode='w') as zf:
            for f in os.listdir(self.download_path):
                if f.endswith('.txt'):
                    zf.write(os.path.join(self.download_path, f), 
                             '%s/%s' % (zf_name, f),
                             compress_type=zipfile.ZIP_DEFLATED)
        
        conn = S3Connection()
        bucket = conn.get_bucket(self.bucket_name)
        k = Key(bucket)
        k.key = '%s.zip' % zf_name
        k.set_contents_from_filename(zf_name)
        k.make_public()
        bucket.copy_key(
            'IL_Campaign_Disclosure_latest.zip', 
            'illinoissunshine-etl', 
            '%s.zip' % zf_name,
            preserve_acl=True)
        

if __name__ == "__main__":
    from sunshine import app_config 
    import argparse

    parser = argparse.ArgumentParser(description='Download and import campaign disclosure data from the IL State Board of Elections.')

    parser.add_argument('--cache', action='store_true',
                   help='Cache downloaded files to S3')
    
    args = parser.parse_args()

    extract = SunshineExtract(ftp_host=app_config.FTP_HOST,
                              ftp_path=app_config.FTP_PATH,
                              ftp_user=app_config.FTP_USER,
                              ftp_pw=app_config.FTP_PW,
                              aws_key=app_config.AWS_KEY,
                              aws_secret=app_config.AWS_SECRET)
    
    extract.download(cache=args.cache)

    del extract
