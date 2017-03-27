import argparse
import db_api
import db_ops
import math
import requests
import sys
from timeit import default_timer as timer

db = 'http://bisque-1079154594.us-west-2.elb.amazonaws.com/'

def constructDimsUrl(imid, sizex, sizey, atlas):
    url = db + 'image_service/image/' + imid + '?slice=,,,1&resize=' + str(sizex) + ',' + str(sizey)
    url += ',BC,MX&'
    if atlas:
        url += 'textureatlas&'
    url += 'dims'
    return url

def constructAtlasUrl(imid, channel_count, channels, sizex, sizey):
    url = db + 'image_service/image/' + imid + '?slice=,,,1&resize=' + str(sizex) + ',' + str(sizey)
    url += ',BC,MX&textureatlas&depth=8,d,u&fuse='
    assert(len(channels) == channel_count)
    for i in range(0, len(channels)):
        url += str(channels[i][0]) + ',' + str(channels[i][1]) + ',' + str(channels[i][2]) + ';'
    url += ':m&format=png'
    # db + 'image/' + imid + '?slice=,,,1&resize=' + str(max_texture_tile_size.w) + ',' + str(max_texture_tile_size.h) + ',BC,MX&textureatlas&depth=8,d,u&fuse='

    # http://bisque-1079154594.us-west-2.elb.amazonaws.com/image_service/image/00-txi2UgfXMLkqFW6DbQuuQ7?slice=,,,1&resize=341,292,BC,MX&dims
    # http://bisque-1079154594.us-west-2.elb.amazonaws.com/image_service/image/00-txi2UgfXMLkqFW6DbQuuQ7?slice=,,,1&resize=341,292,BC,MX&textureatlas&dims
    # http://bisque-1079154594.us-west-2.elb.amazonaws.com/image_service/image/00-txi2UgfXMLkqFW6DbQuuQ7?slice=,,,1&resize=341,292,BC,MX&textureatlas&depth=8,d,u&fuse=0,0,0;0,0,0;0,0,0;0,0,0;0,0,0;0,0,255;0,255,0;255,0,0;:m&format=png
    # http://bisque-1079154594.us-west-2.elb.amazonaws.com/image_service/image/00-txi2UgfXMLkqFW6DbQuuQ7?slice=,,,1&resize=341,292,BC,MX&textureatlas&depth=8,d,u&fuse=0,0,0;0,0,0;0,0,255;0,255,0;255,0,0;0,0,0;0,0,0;0,0,0;:m&format=png
    # http://bisque-1079154594.us-west-2.elb.amazonaws.com/image_service/image/00-txi2UgfXMLkqFW6DbQuuQ7?slice=,,,1&resize=341,292,BC,MX&textureatlas&depth=8,d,u&fuse=0,255,0;255,0,0;0,0,0;0,0,0;0,0,0;0,0,0;0,0,0;0,0,0;:m&format=png
    return url

def compute_atlas_size(w, h, n):
    # w: image width
    # h: image height
    # n: numbe rof image planes, Z stacks or time points
    # start with atlas composed of a row of images
    ww = w*n
    hh = h
    ratio = float(ww) / float(hh)
    # optimize side to be as close to ratio of 1.0
    for r in range(2, n):
        ipr = math.ceil(float(n) / float(r))
        aw = w*ipr
        ah = h*r
        rr = float(max(aw, ah)) / float(min(aw, ah))
        if rr < ratio:
            ratio = rr
            ww = aw
            hh = ah
        else:
            break
    return [int(round(ww/w)), int(round(hh/h))]

def construct_requests(imid):
    # get meta image_num_c, image_num_x, image_num_y

    # http://bisque-1079154594.us-west-2.elb.amazonaws.com/image_service/00-txi2UgfXMLkqFW6DbQuuQ7?meta
    # must wait for this data
    metaxml = db_api.DbApi.getImageMetadata(imid)
    x = metaxml.find(".//tag[@name='image_num_x']").get('value')
    x = int(x)
    y = metaxml.find(".//tag[@name='image_num_y']").get('value')
    y = int(y)
    z = metaxml.find(".//tag[@name='image_num_z']").get('value')
    z = int(z)
    c = metaxml.find(".//tag[@name='image_num_c']").get('value')
    c = int(c)

    atlasDims = compute_atlas_size(x, y, z)
    maxTexture = 2048
    resizeX = int(math.floor(2048/atlasDims[0]))
    resizeY = int(math.floor(2048/atlasDims[1]))
    # this goes in the resize options
    # max_texture_tile_size = { w: resizeX, h: resizeY };



    # max of 3 for r,g,b channels
    batchSize = 3 # try 2 or 1 to test perf
    batchColors = [[255,0,0], [0,255,0], [0,0,255]]
    channelurls = []
    # generate channel urls

    # http://bisque-1079154594.us-west-2.elb.amazonaws.com/image_service/image/00-txi2UgfXMLkqFW6DbQuuQ7?slice=,,,1&resize=341,292,BC,MX&dims
    channelurls.append(constructDimsUrl(imid=imid, sizex=resizeX, sizey=resizeY, atlas=False))
    # http://bisque-1079154594.us-west-2.elb.amazonaws.com/image_service/image/00-txi2UgfXMLkqFW6DbQuuQ7?slice=,,,1&resize=341,292,BC,MX&textureatlas&dims
    channelurls.append(constructDimsUrl(imid=imid, sizex=resizeX, sizey=resizeY, atlas=True))


    # group channels batchSize at a time, to receive them in the r,g,b channels
    channel_count = c
    nbatches = int(math.ceil(float(channel_count)/batchSize))
    for i in range(0, nbatches):
        channelmask = []
        # init all colors to 0
        for j in range(0, channel_count):
            channelmask.append([0,0,0])
        # now assign r,g,b for each batch
        # the batch array contains the channel indices
        batch = []
        for j in range(0, batchSize):
            # load channels in reverse order because I know that segmentation channels are
            # at the end of the list, compress better and will arrive sooner
            index = channel_count-1 - (i*batchSize+j)
            # the if is for last loop iteration when channel_count % batchSize != 0
            if index < channel_count and index >= 0:
                channelmask[index] = batchColors[j]
                batch.append(index)
        # load channel data at "data range" - every channel rescaled to its min/max
        channelurls.append(constructAtlasUrl(imid=imid, channel_count=c, channels=channelmask, sizex=resizeX, sizey=resizeY))

        # db + 'image/' + imid + '?slice=,,,1&resize=' + str(max_texture_tile_size.w) + ',' + str(max_texture_tile_size.h) + ',BC,MX&textureatlas&depth=8,d,u&fuse='

        # http://bisque-1079154594.us-west-2.elb.amazonaws.com/image_service/image/00-txi2UgfXMLkqFW6DbQuuQ7?slice=,,,1&resize=341,292,BC,MX&dims
        # http://bisque-1079154594.us-west-2.elb.amazonaws.com/image_service/image/00-txi2UgfXMLkqFW6DbQuuQ7?slice=,,,1&resize=341,292,BC,MX&textureatlas&dims
        # http://bisque-1079154594.us-west-2.elb.amazonaws.com/image_service/image/00-txi2UgfXMLkqFW6DbQuuQ7?slice=,,,1&resize=341,292,BC,MX&textureatlas&depth=8,d,u&fuse=0,0,0;0,0,0;0,0,0;0,0,0;0,0,0;0,0,255;0,255,0;255,0,0;:m&format=png
        # http://bisque-1079154594.us-west-2.elb.amazonaws.com/image_service/image/00-txi2UgfXMLkqFW6DbQuuQ7?slice=,,,1&resize=341,292,BC,MX&textureatlas&depth=8,d,u&fuse=0,0,0;0,0,0;0,0,255;0,255,0;255,0,0;0,0,0;0,0,0;0,0,0;:m&format=png
        # http://bisque-1079154594.us-west-2.elb.amazonaws.com/image_service/image/00-txi2UgfXMLkqFW6DbQuuQ7?slice=,,,1&resize=341,292,BC,MX&textureatlas&depth=8,d,u&fuse=0,255,0;255,0,0;0,0,0;0,0,0;0,0,0;0,0,0;0,0,0;0,0,0;:m&format=png
    return channelurls

def issue_requests(reqs, async=False):
    #foo
    for i in range(0, len(reqs)):
        try:
            response = requests.get(reqs[i], headers=db_api.DbApi.headers, verify=False, auth=db_api.DbApi.db_auth)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(e)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('input', help='db uri', nargs='?', default='10.128.62.104')
    parser.add_argument('--image', '-i', help='name match', default='*')
    parser.add_argument('--async', '-a', help='do not wait for requests', default='*')
    parser.add_argument('--num', '-n', help='how many requests, defaults to one per image', default='0')
    args = parser.parse_args()

    session_dict = {
        # 'root': 'http://test-aics-01',
        # 'root': 'http://bisque-00.corp.alleninstitute.org:8080',
        # 'root': 'http://10.128.62.104:8080',
        'root': db,
        'user': 'admin',
        'password': 'admin'
    }

    db_api.DbApi.setSessionInfo(session_dict)

    print('Gathering image ids...')
    # all of them
    xml = db_api.DbApi.getImagesByName('*')
    # first one
    # xml = db_api.DbApi.getImagesByName('*', 1)
    print 'Retrieved ' + str(len(xml.getchildren())) + ' images.'
    n = 0
    for i in xml:
        imid = i.get("resource_uniq")
        reqs = construct_requests(imid)
        start = timer()
        issue_requests(reqs, async=False)
        end = timer()
        print(str(n) + ' : ' + i.get('name') + ' : ' + imid + ' : ' + str(end-start) + 's')
        n = n + 1



if __name__ == "__main__":
    print (sys.argv)
    main()
    sys.exit(0)
