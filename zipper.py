from boto.s3.connection import S3Connection
from boto.s3.key import Key
from io import BytesIO
import zipfile
import os
from datetime import datetime

if __name__ == "__main__":
    from sunshine import app_config 
    
    conn = S3Connection(app_config.AWS_KEY, app_config.AWS_SECRET)
    
    outp = BytesIO()
    now = datetime.now().strftime('%Y-%m-%d')
    zf_name = 'IL_Elections_%s' % now
    with zipfile.ZipFile(outp, mode='w') as zf:
        for f in os.listdir('downloads'):
            if f != '.gitkeep':
                
                zf.writestr('%s/%s' % (zf_name, f), 
                    open(os.path.join('downloads', f), 'rb').read(), 
                    compress_type=zipfile.ZIP_DEFLATED)
    
    with open('latest.zip', 'wb') as f:
        f.write(outp.getvalue())

    bucket = conn.get_bucket('il-elections')
    k = Key(bucket)
    k.key = '%s.zip' % zf_name
    outp.seek(0)
    k.set_contents_from_filename('latest.zip')
    k.make_public()
    bucket.copy_key(
        'IL_Elections_latest.zip', 
        'il-elections', 
        '%s.zip' % zf_name,
        preserve_acl=True)
