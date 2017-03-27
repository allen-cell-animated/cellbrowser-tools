#!/usr/bin/env python

try:
    from lxml import etree
except ImportError:
    import xml.etree.ElementTree as etree
import db_api
import os


# TODO encode this table in db or someplace else?
proteinToStructure = {
    'ALPHAACTININ': ['Alpha-actinin', 'Actin bundles'],

    'PAXILLIN': ['Paxillin', 'Cell matrix adhesions'],
    'PAXILIN': ['Paxillin', 'Cell matrix adhesions'],

    'TOM20': ['Tom20', 'Mitochondria'],
    'TOMM20': ['Tom20', 'Mitochondria'],

    'ALPHATUBULIN': ['Alpha-tubulin', 'Microtubules'],
    'TUBA1B': ['Alpha-tubulin', 'Microtubules'],

    'LAMINB1': ['Lamin B1', 'Nuclear envelope'],
    'LMNB1': ['Lamin B1', 'Nuclear envelope'],

    'DESMOPLAKIN': ['Desmoplakin', 'Desmosomes'],
    'DSP': ['Desmoplakin', 'Desmosomes'],

    'SEC61BETA': ['Sec61-beta', 'Endoplasmic reticulum'],
    'SEC61B': ['Sec61-beta', 'Endoplasmic reticulum'],

    'FIBRILLARIN': ['Fibrillarin', 'Nucleolus'],
    'FBL': ['Fibrillarin', 'Nucleolus'],

    'BETAACTIN': ['Beta-actin', 'Actin'],
    'ACTB': ['Beta-actin', 'Actin'],

    'ZO1': ['Tight junction ZO1', 'Tight junctions'],
    'TJP1': ['Tight junction ZO1', 'Tight junctions'],

    'MYOSINIIB': ['Non-muscle myosin IIB', 'Actomyosin'],
    'MYH10': ['Non-muscle myosin IIB', 'Actomyosin'],
}

# create xml bundle for the bisque database entry pointing to this image.
# dict should have: source,xmin,xmax,ymin,ymax,zmin,zmax,imageName,imagePath,thumbnailPath
def oneUp(sessionInfo, dict, outfile):
    api = db_api.DbApi()
    api.setSessionInfo(sessionInfo)

    cbrSegmentationVersion = dict['Version']
    cbrStructureSegmentationMethod = dict['StructureSegmentationMethod']
    cbrImageLocation = dict['cbrImageLocation']
    cbrThumbnailURL = dict['cbrThumbnailURL']
    cbrCellName = dict['cbrCellName']
    structureProteinName = dict['structureProteinName']
    cbrDataRoot = dict['cbrDataRoot']
    cbrThumbnailWebRoot = dict['cbrThumbnailWebRoot']

    # strip spaces and hyphens for dictionary lookup.
    structureProteinKey = structureProteinName.replace('-', '').replace(' ', '').replace(',', '').upper()
    structureDisplayName = 'Unknown'
    proteinDisplayName = 'Unknown'
    structureNamePair = proteinToStructure.get(structureProteinKey)
    if structureNamePair is not None:
        structureDisplayName = structureNamePair[1]
        proteinDisplayName = structureNamePair[0]
    else:
        print('Unknown structure protein name: ' + structureProteinName)

    # Pass permission explicitly for each tag. This is to work around an apparent bug in the bisque back-end.
    # If we upgrade the back end we should check to see if this bug is fixed. The tag permissions should be inherited
    # from the parent.
    perm = 'published'

    tifext = '.ome.tif'
    fullpath = cbrImageLocation + '/' + cbrCellName + tifext
    relpath = fullpath.replace(cbrDataRoot, '')
    # assume thumbnail to be a png file and servable from thumbnailpath
    thumbnail = cbrThumbnailURL
    relpath_thumbnail = thumbnail.replace(cbrThumbnailWebRoot, '')
    resource = etree.Element('image',
                             name=cbrCellName + tifext,
                             value=relpath)
    resource.set('permission', perm)

    etree.SubElement(resource, 'tag', name='name', value=cbrCellName, permission=perm)
    # filename is auto inserted by bisque
    # etree.SubElement(resource, 'tag', name='filename', value=cbrCellName+tifext, permission=perm)

    etree.SubElement(resource, 'tag', name='thumbnail', value=relpath_thumbnail, permission=perm)
    etree.SubElement(resource, 'tag', name='structureProteinName', value=proteinDisplayName, permission=perm)
    etree.SubElement(resource, 'tag', name='structureName', value=structureDisplayName, permission=perm)
    # this batch of images are all from microscope and not simulated.
    etree.SubElement(resource, 'tag', name='isModel', value='false', permission=perm)
    etree.SubElement(resource, 'tag', name='segmentationVersion', value=cbrSegmentationVersion, permission=perm)
    if cbrStructureSegmentationMethod:
        etree.SubElement(resource, 'tag', name='structureSegmentationMethod', value=cbrStructureSegmentationMethod, permission=perm)
    if dict['inputFilename']:
        etree.SubElement(resource, 'tag', name='inputFilename', value=dict['inputFilename'], permission=perm)

    # add a tag for each channel name, by index
    if dict['channelNames'] is not None:
        channel_names = dict['channelNames']
        for i in range(len(channel_names)):
            etree.SubElement(resource, 'tag', name='channelLabel_'+str(i), value=channel_names[i], permission=perm)

    # assume bounding box exists...
    if dict['cbrBounds'] is not None:
        b = dict['cbrBounds']
        # just a comma delimited string
        bounds = str(b['xmin']) + ',' + str(b['xmax']) + ',' + str(b['ymin']) + ',' + str(b['ymax']) + ',' + str(b['zmin']) + ',' + str(b['zmax'])
        etree.SubElement(resource, 'tag', name='bounds', value=bounds, permission=perm)
    if dict['cbrSourceImageName'] is not None:
        etree.SubElement(resource, 'tag', name='source', value=dict['cbrSourceImageName'], permission=perm)
        etree.SubElement(resource, 'tag', name='isCropped', value="true", permission=perm)
    else:
        etree.SubElement(resource, 'tag', name='isCropped', value="false", permission=perm)

    resource_uniq = api.add_image(resource)
    print(cbrCellName + ',' + (resource_uniq if resource_uniq is not None else "None") + ',' + fullpath)

    if outfile is not None:
        outfile.write(cbrCellName + ',' + resource_uniq + ',' + fullpath + os.linesep)
    return resource_uniq