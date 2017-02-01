import requests
from xml.etree import ElementTree


class DbApi(object):

    headers = {
        'Content-type': 'text/xml',
    }

    db_uri = 'http://SPECIFY_DB_URL_EXPLICITLY'  # 'http://10.128.62.104/data_service/'
    db_auth = ('admin', 'admin')

    @staticmethod
    def setSessionInfo(session_dict):
        if session_dict is None:
            session_dict = {
                'root': 'http://SPECIFY_DB_URL_EXPLICITLY',
                'user': 'admin',
                'password': 'admin'
            }
        DbApi.db_uri = session_dict['root'] + '/data_service/'
        DbApi.db_auth = (session_dict['user'], session_dict['password'])

    # data = open('edit.xml')
    # requests.post('http://10.128.62.104:8080/data_service/00-XrD4eGZZtzJ98MBrxokUt4', headers=DbApi.headers, data=data, verify=False, auth=DbApi.auth)
    @staticmethod
    def addTag(imgId, name, value):
        # http://10.128.62.104:8080/client_service/view?resource=http://10.128.62.104:8080/data_service/00-iPDrkt4dZaL2uWLoCDmQEd
        data = '<image uri=\'/data_service/image/' + imgId + '\' ><tag name="' + name + '" value="' + value + '" permission="published"/></image>'
        try:
            response = requests.post(DbApi.db_uri + imgId, headers=DbApi.headers, data=data, verify=False, auth=DbApi.db_auth)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(e)

    # update tag value for a tag name within a set of images
    # db_api.DbApi.updateTag('20160719_S01*', "structureName", "Nucleus")
    @staticmethod
    def updateTag(imgname, tagname, value):
        images = DbApi.getImagesByName(imgname)
        for i in images:
            for tag in i.findall("tag"):
                if tag.get('name') == tagname:
                    taguri = tag.get("uri")
                    # string after last slash is tag id
                    tagid = taguri.split('/')[-1]
                    data = '<tag uri=\'/data_service/tag/' + tagid + '\' value="' + value + '" permission="published"></tag>'
                    try:
                        response = requests.put(taguri, headers=DbApi.headers, data=data, verify=False, auth=DbApi.db_auth)
                        response.raise_for_status()
                    except requests.exceptions.RequestException as e:
                        print(e)

    # get all images:
    # GET http://10.128.62.104:8080/data_service/image/?view=deep
    # get all image tags:
    # GET http://10.128.62.104:8080/data_service/image/00-iPDrkt4dZaL2uWLoCDmQEd?view=deep
    @staticmethod
    def getImagesByName(name):
        results = ElementTree.Element('results')
        limit = '700'
        nlimit = 700
        n = 0
        more = True
        while more:
            try:
                response = requests.get(DbApi.db_uri + 'image/?offset='+str(n)+'&tag_query=name:' + name + '&tag_order="@ts":desc&wpublic=false&limit=' + limit + '&view=deep', headers=DbApi.headers, verify=False, auth=DbApi.db_auth)
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                print(e)
            tree = ElementTree.fromstring(response.content)
            if tree is not None:
                results.extend(tree.getchildren())
                count = len(tree.getchildren())
                # if more than 700 returned, loop around and get 700 more until we have all query results
                if count < nlimit:
                    more = False
                n += count
            else:
                more = False
        return results

    # Querying resources
    #
    # [[type:]name:]value
    # Wildcard: *
    # Expressions: and, or, ()
    # Attributes: @
    # Attributes include: name, value, type, hidden, ts, created
    #
    # Examples
    # Find anything containing string "GFP": *GFP*
    # Find a resource with value "GFP": :GFP
    # Find a resource with name "antibody" and value "GFP": antibody:GFP
    # Find a resource with name "antibody" and value containing "GFP": antibody:*GFP*
    # Find a resource with type "seed": seed::
    # Find a resource with type "seed" and name "seed1": seed:seed1:
    #
    # Expression examples
    # Find a resource with name "antibody" and value "GFP" and type "seed": antibody:GFP and seed::
    # Find a resource with name "antibody" and value "GFP" or type "seed": antibody:GFP or seed::
    # Find a resource with name "antibody" and value "GFP" or type "seed": (antibody:GFP or seed::) and antibody:Beta
    #
    # <h3>Attribute examples</h3>
    # <li>Find a resource with time stamp "2012:01:01": <b>@ts=&gt;2012:01:01</b></li>
    @staticmethod
    def getImagesByTagValue(name, value):
        results = ElementTree.Element('results')
        limit = '700'
        nlimit = 700
        n = 0
        more = True
        while more:
            try:
                response = requests.get(DbApi.db_uri + 'image/?offset='+str(n)+'&tag_query=' + name + ':' + value + '&tag_order="@ts":desc&wpublic=false&limit=' + limit + '&view=deep', headers=DbApi.headers, verify=False, auth=DbApi.db_auth)
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                print(e)
            tree = ElementTree.fromstring(response.content)
            if tree is not None:
                results.extend(tree.getchildren())
                count = len(tree.getchildren())
                # if more than 700 returned, loop around and get 700 more until we have all query results
                if count < nlimit:
                    more = False
                n += count
            else:
                more = False
        return results

    # get all images:
    # GET http://10.128.62.104:8080/data_service/image/?view=deep
    # get all image tags:
    # GET http://10.128.62.104:8080/data_service/image/00-iPDrkt4dZaL2uWLoCDmQEd?view=deep
    @staticmethod
    def forEachImageByName(name, visitor):
        limit = '700'
        nlimit = 700
        more = True
        n = 0
        while more:
            try:
                response = requests.get(DbApi.db_uri + 'image/?offset='+str(n)+'&tag_query=name:' + name + '&tag_order="@ts":desc&wpublic=false&limit=' + limit + '&view=deep', headers=DbApi.headers, verify=False, auth=DbApi.db_auth)
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                print(e)
            tree = ElementTree.fromstring(response.content)
            if tree is not None:
                for i in tree:
                    visitor(i)
                count = len(tree.getchildren())
                # if more than 700 returned, loop around and get 700 more until we have all query results
                if count < nlimit:
                    more = False
                n += count
            else:
                more = False
        return n

    @staticmethod
    def getImageIdFromName(name):
        i = DbApi.getImagesByName(name)
        if i is not None:
            for image in i:
                # return the FIRST ONE ONLY
                return image.get("resource_uniq")
        return None

    @staticmethod
    def getValuesForTagName(name):
        # DbApi.db_uri + 'image/?extract=tag[value,%20name=%22'+name+'%22]'
        try:
            response = requests.get(DbApi.db_uri + 'image/?tag_values=' + name, headers=DbApi.headers, verify=False, auth=DbApi.db_auth)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(e)

    @staticmethod
    def deleteTagUri(taguri):
        try:
            response = requests.delete(taguri, headers=DbApi.headers, verify=False, auth=DbApi.db_auth)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(e)

    @staticmethod
    def deleteTagsByName(imgname, tagname):
        images = DbApi.getImagesByName(imgname)
        for i in images:
            for tag in i.findall("tag"):
                if tag.get('name') == tagname:
                    taguri = tag.get("uri")
                    DbApi.deleteTagUri(taguri)

    @staticmethod
    def deleteImageByName(imgname):
        imgid = DbApi.getImageIdFromName(imgname)
        if imgid:
            try:
                response = requests.delete(DbApi.db_uri + imgid, headers=DbApi.headers, verify=False, auth=DbApi.db_auth)
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                print(e)

    @staticmethod
    def deleteImage(imgId):
        try:
            response = requests.delete(DbApi.db_uri + imgId, headers=DbApi.headers, verify=False, auth=DbApi.db_auth)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(e)

    @staticmethod
    def add_image(xml):
        # http://10.128.62.104:8080/client_service/view?resource=http://10.128.62.104:8080/data_service/00-iPDrkt4dZaL2uWLoCDmQEd
        data = ElementTree.toString(xml)
        try:
            response = requests.post(DbApi.db_uri, headers=DbApi.headers, data=data, verify=False, auth=DbApi.db_auth)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(e)
            raise
        tree = ElementTree.fromstring(response.content)
        if tree is None or tree.get('uri') is None:
            print('Upload failed')
            print(response.content)
            print(data)
            return None
        else:
            print('Uploaded ID: %s, URL: %s' % (tree.get('resource_uniq'), tree.get('uri')))
            # don't store url
            # f['url'] = r.get('uri')
            return tree.get('resource_uniq')
