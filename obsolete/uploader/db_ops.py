import db_api


# DELETE A SET OF IMAGES
# deleteImagesByName('20160708_C01*')
def deleteImagesByName(namestr):
    xml = db_api.DbApi.getImagesByName(namestr)
    print('Retrieved ' + str(len(xml.getchildren())) + ' images.')
    for i in xml:
        imid = i.get("resource_uniq")
        print(imid)
        db_api.DbApi.deleteImage(imid)


def deleteDuplicateImagesByName(namestr):
    xml = db_api.DbApi.getImagesByName(namestr)
    print('Retrieved ' + str(len(xml.getchildren())) + ' images.')
    names = []
    for i in xml:
        imname = i.get("name")
        if imname in names:
            imid = i.get("resource_uniq")
            print(imid)
            db_api.DbApi.deleteImage(imid)
        else:
            names.append(imname)


def deleteImagesByTagValue(tag, value):
    xml = db_api.DbApi.getImagesByTagValue(tag, value)
    print('Retrieved ' + str(len(xml.getchildren())) + ' images.')
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


def countImagesByName(namestr):
    xml = db_api.DbApi.getImagesByName(namestr)
    print('Retrieved ' + str(len(xml.getchildren())) + ' images.')
