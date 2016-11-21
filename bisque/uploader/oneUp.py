#!/usr/bin/env python

try:
    from lxml import etree
except ImportError:
    import xml.etree.ElementTree as etree
import os
import uploader


# create xml bundle for the bisque database entry pointing to this image.
# dict should have: source,xmin,xmax,ymin,ymax,zmin,zmax,imageName,imagePath,thumbnailPath
def oneUp(sessionInfo, dict, outfile):
    session = uploader.init(sessionInfo)

    cbrImageLocation = dict['cbrImageLocation']
    cbrThumbnailLocation = dict['cbrThumbnailLocation']
    cbrCellName = dict['cbrCellName']
    structureProteinName = dict['structureProteinName']

    fullpath = cbrImageLocation + cbrCellName + '.ome.tif'
    # assume thumbnail to be a png file and servable from thumbnailpath
    thumbnail = cbrThumbnailLocation + cbrCellName + '.png'
    resource = etree.Element('image',
                             name=cbrCellName+'.ome.tif',
                             value=fullpath)
    resource.set('permission', 'published')

    etree.SubElement(resource, 'tag', name='name', value=cbrCellName)
    etree.SubElement(resource, 'tag', name='filename', value=cbrCellName)

    etree.SubElement(resource, 'tag', name='thumbnail', value=thumbnail)
    etree.SubElement(resource, 'tag', name='structureName', value=structureProteinName)

    # assume bounding box exists...
    if dict['cbrBounds'] is not None:
        b = dict['cbrBounds']
        # just a comma delimited string
        bounds = str(b['xmin'])+','+str(b['xmax'])+','+str(b['ymin'])+','+str(b['ymax'])+','+str(b['zmin'])+','+str(b['zmax'])
        etree.SubElement(resource, 'tag', name='bounds', value=bounds)
    if dict['cbrSourceImageName'] is not None:
        etree.SubElement(resource, 'tag', name='source', value=dict['cbrSourceImageName'])

    resource_uniq = uploader.uploadFileSpec(session, resource, None)
    print cbrCellName + ',' + (resource_uniq if resource_uniq is not None else "None") + ',' + fullpath

    if outfile is not None:
        outfile.write(cbrCellName + ',' + resource_uniq + ',' + fullpath + os.linesep)
    return resource_uniq
