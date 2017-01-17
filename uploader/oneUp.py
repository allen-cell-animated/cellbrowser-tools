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
    cbrThumbnailURL = dict['cbrThumbnailURL']
    cbrCellName = dict['cbrCellName']
    structureProteinName = dict['structureProteinName']

    proteinToStructure = {
        'Alpha actinin': 'Membrane',
        'Alpha-actinin': 'Membrane',
        'Paxillin': 'Adhesions',
        'TOM20': 'Mitochondria',
        'Tom20': 'Mitochondria',
        'Alpha tubulin': 'Microtubules',
        'Alpha-tubulin': 'Microtubules',
        'LaminB1': 'Nucleus',
        'Lamin-B1': 'Nucleus',
        'Desmoplakin': 'Cell-cell junctions',
        'Sec61 beta': 'Endoplasmic reticulum',
        'Sec61-beta': 'Endoplasmic reticulum',
        'Fibrillarin': 'Nucleolus',
        'Beta actin': 'Actin',
        'Beta-actin': 'Actin',
        'Vimentin': 'Intermediate filaments',
        'LAMP1': 'Lysosome',
        'ZO 1': 'Tight junctions',
        'ZO-1': 'Tight junctions',
        'ZO1': 'Tight junctions',
        'Myosin IIB': 'Myosin',
        'beta-galactoside alpha-2,6-sialyltransferase 1': 'Golgi',
        'ST6GAL1': 'Golgi',
        'LC3': 'Autophagosomes',
        'Centrin': 'Centrosome',
        'GFP': 'Cytoplasm'
    }
    structureName = proteinToStructure.get(structureProteinName)
    if structureName is None:
        structureName = "Unknown"

    # Pass permission explicitly for each tag. This is to work around an apparent bug in the bisque back-end.
    # If we upgrade the back end we should check to see if this bug is fixed. The tag permissions should be inherited
    # from the parent.
    perm = 'published'

    tifext = '.ome.tif'
    fullpath = cbrImageLocation + '/' + cbrCellName + tifext
    # assume thumbnail to be a png file and servable from thumbnailpath
    thumbnail = cbrThumbnailURL
    resource = etree.Element('image',
                             name=cbrCellName+tifext,
                             value=fullpath)
    resource.set('permission', perm)

    etree.SubElement(resource, 'tag', name='name', value=cbrCellName, permission=perm)
    etree.SubElement(resource, 'tag', name='filename', value=cbrCellName+tifext, permission=perm)

    etree.SubElement(resource, 'tag', name='thumbnail', value=thumbnail, permission=perm)
    etree.SubElement(resource, 'tag', name='structureProteinName', value=structureProteinName, permission=perm)
    etree.SubElement(resource, 'tag', name='structureName', value=structureName, permission=perm)

    # assume bounding box exists...
    if dict['cbrBounds'] is not None:
        b = dict['cbrBounds']
        # just a comma delimited string
        bounds = str(b['xmin'])+','+str(b['xmax'])+','+str(b['ymin'])+','+str(b['ymax'])+','+str(b['zmin'])+','+str(b['zmax'])
        etree.SubElement(resource, 'tag', name='bounds', value=bounds, permission=perm)
    if dict['cbrSourceImageName'] is not None:
        etree.SubElement(resource, 'tag', name='source', value=dict['cbrSourceImageName'], permission=perm)

    resource_uniq = uploader.uploadFileSpec(session, resource, None)
    print cbrCellName + ',' + (resource_uniq if resource_uniq is not None else "None") + ',' + fullpath

    if outfile is not None:
        outfile.write(cbrCellName + ',' + resource_uniq + ',' + fullpath + os.linesep)
    return resource_uniq
