import os

try:
    from lxml import etree
except ImportError:
    import xml.etree.ElementTree as etree

# Include the Bisque API
try:
    from bqapi import BQSession, BQCommError
except ImportError:
    print 'please install the bisque api \n pip install bisque-api'

from bqapi.util import save_blob, localpath2url

# THE UPLOADER.

def init():
    # user and password
    root = 'http://10.128.62.45:8080'
    user = 'admin'
    pswd = 'admin'
    session = BQSession().init_local(user, pswd, bisque_root=root, create_mex=False)
    return session

def uploadFileSpec(session, fxml, filepath=None):
    r = etree.XML(session.postblob(filepath, xml=fxml)).find('./')

    if r is None or r.get('uri') is None:
        print 'Upload failed'
        return None
    else:
        print 'Uploaded ID: %s, URL: %s' % (r.get('resource_uniq'), r.get('uri'))
        # don't store url
        # f['url'] = r.get('uri')
        return r.get('resource_uniq')
