import argparse
import db_api
import db_ops
import sys


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('input', help='input name query')
    args = parser.parse_args()

    session_dict = {
        # 'root': 'http://test-aics-01',
        # 'root': 'http://bisque-00.corp.alleninstitute.org:8080',
        # 'root': 'http://10.128.62.104:8080',
        'root': 'http://10.128.62.104',
        'user': 'admin',
        'password': 'admin'
    }

    db_api.DbApi.setSessionInfo(session_dict)
    # db_ops.countImagesByName(args.input)
    xml = db_api.DbApi.getImagesByTagValue("isCropped", "true")
    print 'Retrieved ' + str(len(xml.getchildren())) + ' images with "isCropped" true.'
    srcdict = {}
    for element in xml:
        child_el = element.find('tag[@name="source"]')
        src = child_el.attrib['value']
        # TODO find a nicer way to get this grouping.
        # strip away all after the last underscore.
        if src.startswith('35000'):
            src = src[:src.index('_')]
        else:
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
        if src.startswith('35000'):
            src = src[:src.index('_')]
        else:
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
