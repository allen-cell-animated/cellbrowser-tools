import db_api

session_dict = {
    # 'root': 'http://test-aics-01',
    # 'root': 'http://bisque-00.corp.alleninstitute.org:8080',
    # 'root': 'http://10.128.62.104:8080',
    'root': 'http://10.128.62.104',
    'user': 'admin',
    'password': 'admin'
}

db_api.DbApi.setSessionInfo(session_dict)


# DELETE A SET OF IMAGES
# deleteImagesByName('20160708_C01*')
def deleteImagesByName(namestr):
    xml = db_api.DbApi.getImagesByName(namestr)
    for i in xml:
        imid = i.get("resource_uniq")
        print(imid)
        db_api.DbApi.deleteImage(imid)


def removeRedundantTags():
    def visitor(xml):
        visitor.counter += 1
        if visitor.counter % 10 == 0:
            print(str(visitor.counter))
        tags = xml.findall('.//tag')
        i = 0
        for tag in tags:
            if i > 0:
                taguri = tag.get("uri")
                db_api.DbApi.deleteTagUri(taguri)
            i += 1
    visitor.counter = 0
    db_api.DbApi.forEachImageByName('*', visitor)

def addIsCroppedTag():
    def visitor(xml):
        cropped = xml.findall('.//tag[@name="isCropped"]')
        visitor.counter += 1
        if visitor.counter % 10 == 0:
            print(str(visitor.counter))
        if not cropped:
            if xml.findall('.//tag[@name="source"]'):
                db_api.DbApi.addTag(xml.get("resource_uniq"), "isCropped", "true")
            else:
                db_api.DbApi.addTag(xml.get("resource_uniq"), "isCropped", "false")
    visitor.counter = 0
    db_api.DbApi.forEachImageByName('*', visitor)
