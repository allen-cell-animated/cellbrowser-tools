#!/usr/bin/env python

try:
    from lxml import etree
except ImportError:
    import xml.etree.ElementTree as etree
import db_api
import os


# create xml bundle for the bisque database entry pointing to this image.
# dict should have: source,xmin,xmax,ymin,ymax,zmin,zmax,imageName,imagePath,thumbnailPath
def oneUp(sessionInfo, dict, outfile):
    db_api.setSessionInfo(sessionInfo)

    cbrImageLocation = dict['cbrImageLocation']
    cbrThumbnailURL = dict['cbrThumbnailURL']
    cbrCellName = dict['cbrCellName']
    structureProteinName = dict['structureProteinName']

    # TODO encode this table in db or someplace else?
    proteinToStructure = {
        'ALPHAACTININ': 'Membrane',
        'PAXILLIN': 'Adhesions',
        'PAXILIN': 'Adhesions',
        'TOM20': 'Mitochondria',
        'ALPHATUBULIN': 'Microtubules',
        'LAMINB1': 'Nucleus',
        'DESMOPLAKIN': 'Cell-cell junctions',
        'SEC61BETA': 'Endoplasmic reticulum',
        'SEC61B': 'Endoplasmic reticulum',
        'FIBRILLARIN': 'Nucleolus',
        'BETAACTIN': 'Actin',
        'VIMENTIN': 'Intermediate filaments',
        'LAMP1': 'Lysosome',
        'ZO1': 'Tight junctions',
        'MYOSINIIB': 'Myosin',
        'BETAGALACTOSIDEALPHA26SIALYLTRANSFERASE1': 'Golgi',
        'ST6GAL1': 'Golgi',
        'LC3': 'Autophagosomes',
        'CENTRIN': 'Centrosome',
        'GFP': 'Cytoplasm',
        'PMP34': 'Peroxisomes',
        'CAAX': 'Plasma membrane'
    }
    # strip spaces and hyphens for dictionary lookup.
    structureProteinKey = structureProteinName.replace('-', '').replace(' ', '').replace(',', '').upper()
    structureName = proteinToStructure.get(structureProteinKey)
    if structureName is None:
        structureName = "Unknown"
        print('Unknown structure protein name: ' + structureProteinName)

    # Pass permission explicitly for each tag. This is to work around an apparent bug in the bisque back-end.
    # If we upgrade the back end we should check to see if this bug is fixed. The tag permissions should be inherited
    # from the parent.
    perm = 'published'

    tifext = '.ome.tif'
    fullpath = cbrImageLocation + '/' + cbrCellName + tifext
    # assume thumbnail to be a png file and servable from thumbnailpath
    thumbnail = cbrThumbnailURL
    resource = etree.Element('image',
                             name=cbrCellName + tifext,
                             value=fullpath)
    resource.set('permission', perm)

    etree.SubElement(resource, 'tag', name='name', value=cbrCellName, permission=perm)
    # filename is auto inserted by bisque
    # etree.SubElement(resource, 'tag', name='filename', value=cbrCellName+tifext, permission=perm)

    etree.SubElement(resource, 'tag', name='thumbnail', value=thumbnail, permission=perm)
    etree.SubElement(resource, 'tag', name='structureProteinName', value=structureProteinName, permission=perm)
    etree.SubElement(resource, 'tag', name='structureName', value=structureName, permission=perm)
    # this batch of images are all from microscope and not simulated.
    etree.SubElement(resource, 'tag', name='isModel', value='false', permission=perm)

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

    resource_uniq = db_api.add_image(resource)
    print(cbrCellName + ',' + (resource_uniq if resource_uniq is not None else "None") + ',' + fullpath)

    if outfile is not None:
        outfile.write(cbrCellName + ',' + resource_uniq + ',' + fullpath + os.linesep)
    return resource_uniq
