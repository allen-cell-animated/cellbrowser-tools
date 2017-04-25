import argparse
import db_api
import db_ops
import sys


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('input', help='db uri', nargs='?', default='10.128.62.104')
    parser.add_argument('--name', '-n', help='name match', default='*')
    args = parser.parse_args()

    session_dict = {
        # 'root': 'http://test-aics-01',
        # 'root': 'http://bisque-00.corp.alleninstitute.org:8080',
        # 'root': 'http://10.128.62.104:8080',
        'root': 'http://'+args.input,
        'user': 'admin',
        'password': 'admin'
    }

    db_api.DbApi.setSessionInfo(session_dict)

    # xml = db_api.DbApi.getImagesByName("20160705_S03*")
    # print 'Retrieved ' + str(len(xml.getchildren())) + ' images with name "21060705_S03*".'
    # names = []
    # for element in xml:
    #     child_el = element.find('tag[@name="isCropped"]')
    #     nm = child_el.attrib['value']
    #     if (nm == 'true'):
    #         child_el = element.find('tag[@name="name"]')
    #         nm = child_el.attrib['value']
    #         names.append(nm)
    # print '\n'.join(sorted(names))
    # return



    # db_ops.countImagesByName(args.input)
    xml = db_api.DbApi.getImagesByTagValue("isCropped", "true")
    print 'Retrieved ' + str(len(xml.getchildren())) + ' images with "isCropped" true.'
    srcdict = {}
    for element in xml:
        child_el = element.find('tag[@name="source"]')
        src = child_el.attrib['value']
        # TODO find a nicer way to get this grouping.
        # strip away all after the last underscore.
        src = src[:src.rindex('_')]
        # add to dict
        if src not in srcdict:
            srcdict[src] = 1
        else:
            srcdict[src] += 1
    for (key, value) in sorted(srcdict.items()):
        print(key+'\t'+str(value))

    xml = db_api.DbApi.getImagesByTagValue("isCropped", "false")
    print 'Retrieved ' + str(len(xml.getchildren())) + ' images with "isCropped" false.'
    srcdict = {}
    for element in xml:
        child_el = element.find('tag[@name="name"]')
        src = child_el.attrib['value']
        # TODO find a nicer way to get this grouping.
        # strip away all after the last underscore.
        src = src[:src.rindex('_')]
        # add to dict
        if src not in srcdict:
            srcdict[src] = 1
        else:
            srcdict[src] += 1
    for (key, value) in sorted(srcdict.items()):
        print(key+'\t'+str(value))

if __name__ == "__main__":
    print (sys.argv)
    main()
    sys.exit(0)
