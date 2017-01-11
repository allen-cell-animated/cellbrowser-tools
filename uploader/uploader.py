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

# sessionDictionary must have root, user, password
def init(sessionDictionary):
    if sessionDictionary is None:
        sessionDictionary = {
            'root': 'http://test-aics-01',
            # 'root': 'http://bisque-00.corp.alleninstitute.org:8080',
            # 'root': 'http://10.128.62.98:8080',
            'user': 'admin',
            'password': 'admin'
        }
    session = BQSession().init_local(sessionDictionary['user'],
                                     sessionDictionary['password'],
                                     bisque_root=sessionDictionary['root'],
                                     create_mex=False)
    return session


def dataServiceURL(session):
    return session.service_url('data_service')


def uploadFileSpec(session, fxml, filepath=None):
    r = etree.XML(session.postblob(filepath, xml=fxml)).find('./')

    if r is None or r.get('uri') is None:
        print 'Upload failed'
        print etree.tostring(r)
        print etree.tostring(fxml)
        return None
    else:
        print 'Uploaded ID: %s, URL: %s' % (r.get('resource_uniq'), r.get('uri'))
        # don't store url
        # f['url'] = r.get('uri')
        return r.get('resource_uniq')


def uploadDataSet(session, dsname, resourceIds):
    ''' upload a named dataset comprised of a list of resourceIds '''
    dataset = etree.Element('dataset', name=dsname)
    for i in resourceIds:
        v = etree.SubElement(dataset, 'value', type='object')
        v.text = dataServiceURL+i
    serviceurl = session.service_url('data_service')
    response = session.postxml(serviceurl, dataset)
    if response is None or response.get('uri') is None:
        print 'Dataset upload failed'
        return None
    else:
        print 'Uploaded Dataset ID: %s, URL: %s' % (response.get('resource_uniq'), response.get('uri'))
