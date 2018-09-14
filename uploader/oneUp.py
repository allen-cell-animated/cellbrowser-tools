#!/usr/bin/env python

try:
    from lxml import etree
except ImportError:
    import xml.etree.ElementTree as etree
from . import db_api
import os

# create xml bundle for the bisque database entry pointing to this image.
# dict should have: source,xmin,xmax,ymin,ymax,zmin,zmax,imageName,imagePath,thumbnailPath
def oneUp(sessionInfo, dict, outfile):
    api = db_api.DbApi()
    api.setSessionInfo(sessionInfo)

    cbrCellSegmentationVersion = dict['NucMembSegmentationAlgorithmVersion']
    cbrStructureSegmentationVersion = dict['StructureSegmentationAlgorithmVersion']
    cbrStructureSegmentationMethod = dict['StructureSegmentationAlgorithm']
    cbrImageLocation = dict['cbrImageLocation']
    cbrThumbnailURL = dict['cbrThumbnailURL']
    cbrCellName = dict['cbrCellName']
    cbrDataRoot = dict['cbrDataRoot']

    cbrColonyPosition = dict['Colony position']
    cbrChannelNames = dict['channelNames']

    proteinDisplayName = dict['structureProteinName']
    structureDisplayName = dict['structureName']

    imageRelPath = dict['cbrImageRelPath']

    inputFilename = dict['SourceFilename']
    sourceImageName = dict['cbrSourceImageName']

    legacyFovNames = dict['LegacyFOVName']

    tifext = '.ome.tif'

    # avoid dups:
    # before adding this entry,
    # destroy any db entries with this name or this inputFilename
    ims = api.getImagesByName(cbrCellName)
    if ims is not None:
        for image in ims:
            api.deleteImage(image.get("resource_uniq"))
    # We can't delete these because all the segmented images have the same inputFilename
    # ims = api.getImagesByTagValue(name='inputFilename', value=dict['inputFilename'])
    # if ims is not None:
    #     for image in ims:
    #         api.deleteImage(image.get("resource_uniq"))

    # Pass permission explicitly for each tag. This is to work around an apparent bug in the bisque back-end.
    # If we upgrade the back end we should check to see if this bug is fixed. The tag permissions should be inherited
    # from the parent.
    perm = 'published'

    relpath = imageRelPath.replace('\\', '/')
    # assume thumbnail to be a png file and servable from thumbnailpath
    thumbnail = cbrThumbnailURL
    relpath_thumbnail = thumbnail
    relpath_ometif = thumbnail.replace('.png', tifext)
    resource = etree.Element('image',
                             name=cbrCellName + tifext,
                             value=relpath + '/' + cbrCellName + tifext)
    resource.set('permission', perm)

    etree.SubElement(resource, 'tag', name='name', value=cbrCellName, permission=perm)
    # filename is auto inserted by bisque
    # etree.SubElement(resource, 'tag', name='filename', value=cbrCellName+tifext, permission=perm)

    etree.SubElement(resource, 'tag', name='thumbnail', value=relpath_thumbnail, permission=perm)
    etree.SubElement(resource, 'tag', name='ometifpath', value=relpath_ometif, permission=perm)
    etree.SubElement(resource, 'tag', name='structureProteinName', value=proteinDisplayName, permission=perm)
    etree.SubElement(resource, 'tag', name='structureName', value=structureDisplayName, permission=perm)
    # this batch of images are all from microscope and not simulated.
    etree.SubElement(resource, 'tag', name='isModel', value='false', permission=perm)
    etree.SubElement(resource, 'tag', name='cellSegmentationVersion', value=str(cbrCellSegmentationVersion), permission=perm)
    etree.SubElement(resource, 'tag', name='structureSegmentationVersion', value=str(cbrStructureSegmentationVersion), permission=perm)
    etree.SubElement(resource, 'tag', name='colony_position', value=str(cbrColonyPosition), permission=perm)
    
    if inputFilename:
        etree.SubElement(resource, 'tag', name='inputFilename', value=inputFilename, permission=perm)

    # add a tag for each channel name, by index
    if cbrChannelNames is not None:
        for i in range(len(cbrChannelNames)):
            etree.SubElement(resource, 'tag', name='channelLabel_'+str(i), value=cbrChannelNames[i], permission=perm)

    if sourceImageName is not None:
        etree.SubElement(resource, 'tag', name='parentometifpath', value=relpath + '/' + sourceImageName + tifext, permission=perm)
        etree.SubElement(resource, 'tag', name='source', value=sourceImageName, permission=perm)
        etree.SubElement(resource, 'tag', name='isCropped', value="true", permission=perm)
    else:
        etree.SubElement(resource, 'tag', name='isCropped', value="false", permission=perm)

    if legacyFovNames is not None:
        for i in legacyFovNames:
            etree.SubElement(resource, 'tag', name='legacyFOVname', value=i, permission=perm)

    resource_uniq = api.add_image(resource)

    fullpath = cbrImageLocation + '/' + cbrCellName + tifext
    print(cbrCellName + ',' + (resource_uniq if resource_uniq is not None else "None") + ',' + fullpath)

    if outfile is not None:
        outfile.write(cbrCellName + ',' + resource_uniq + ',' + fullpath + os.linesep)
    return resource_uniq
