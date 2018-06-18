
class CellJob(object):
    def __init__(self, csvRow):
        self.DeliveryDate = '00/00/0000'
        self.VersionNucMemb = '0.0'
        self.VersionStructure = '0.0'
        self.inputFolder = ''
        self.inputFilename = ''
        self.inputFileRow = 0
        self.cellLineId = 0

        self.xyPixelSize = 0.0
        self.zPixelSize = 0.0

        self.memChannel = 0
        self.nucChannel = 0
        self.structureChannel = 0
        self.structureProteinName = ''
        self.structureName = ''
        self.lightChannel = 0
        self.timePoint = 0
        self.colonyPosition = ''

        self.outputSegmentationPath = ''
        self.outputNucSegWholeFilename = ''
        self.outputCellSegWholeFilename = ''
        self.structureSegOutputFolder = ''
        self.structureSegOutputFilename = ''

        self.outputCellSegIndex = ''

        self.StructureSegmentationMethod = ''

        self.outputSegmentationContourPath = ''
        self.structureSegContourFilename = ''
        self.outputNucSegContourFilename = ''
        self.outputCellSegContourFilename = ''

        self.cbrImageRelPath = 'images'
        self.cbrImageLocation = './images'
        self.cbrTextureAtlasLocation = './atlas'
        self.cbrThumbnailLocation = './images'
        self.cbrThumbnailURL = 'file:///images'
        self.cbrThumbnailSize = 128

        self.cbrDataRoot = '/allen/aics/software/danielt/images/AICS/bisque/'
        self.cbrThumbnailRoot = '/allen/aics/software/danielt/demos/bisque/thumbnails/'

        self.cbrDatasetName = ''
        self.cbrCellName = ''

        self.dbUrl = ''

        # processing

        self.cbrGenerateThumbnail = False
        self.cbrGenerateCellImage = False
        self.cbrAddToDb = False
        self.cbrGenerateSegmentedImages = True
        self.cbrGenerateFullFieldImages = True
        self.cbrParseError = False
        self.cbrSkipStructureSegmentation = False

        if csvRow is not None:
            self.cbrParseError = csvRow.get('cbrParseError', False)
            self.DeliveryDate = csvRow.get('DeliveryDate')
            self.VersionNucMemb = str(csvRow.get('VersionNucMemb', ' '))
            self.VersionStructure = str(csvRow.get('VersionStructure', ' '))
            self.inputFolder = csvRow.get('inputFolder')
            self.inputFilename = csvRow.get('inputFilename')

            # check for either CellLine or cell_line_ID in spreadsheet row,
            # or cellLineId if this was loaded from json
            self.cellLineId = csvRow.get('CellLine')
            if self.cellLineId is None:
                self.cellLineId = csvRow.get('cell_line_ID')
            if self.cellLineId is None:
                self.cellLineId = csvRow.get('cellLineId')
            if self.cellLineId is None:
                self.cbrParseError = True

            try:
                self.xyPixelSize = float(csvRow.get('xyPixelSize', 0))
            except ValueError:
                self.xyPixelSize = 0
                self.cbrParseError = True
            try:
                self.zPixelSize = float(csvRow.get('zPixelSize', 0))
            except ValueError:
                self.zPixelSize = 0
                self.cbrParseError = True
            try:
                self.memChannel = int(float(csvRow.get('memChannel', 0)))
            except ValueError:
                self.memChannel = 0
                self.cbrParseError = True
            try:
                self.nucChannel = int(float(csvRow.get('nucChannel', 0)))
            except ValueError:
                self.nucChannel = 0
                self.cbrParseError = True
            try:
                self.structureChannel = int(float(csvRow.get('structureChannel', 0)))
            except ValueError:
                self.structureChannel = 0
                self.cbrParseError = True
            self.structureProteinName = csvRow.get('structureProteinName')
            self.structureName = csvRow.get('structureName')
            try:
                self.lightChannel = int(float(csvRow.get('lightChannel', 0)))
            except ValueError:
                self.lightChannel = 0
                self.cbrParseError = True
            try:
                self.timePoint = int(float(csvRow.get('timePoint', 0)))
            except ValueError:
                self.timePoint = 0
                self.cbrParseError = True

            self.colonyPosition = str(csvRow.get('colony_position', csvRow.get('colonyPosition', ' ')))

            # print(self.colonyPosition)
            self.colonyPosition = self.colonyPosition.strip()
            if self.colonyPosition == 'c' or self.colonyPosition == 'center':
                self.colonyPosition = 'center'
            elif self.colonyPosition == 'r' or self.colonyPosition == 'ridge':
                self.colonyPosition = 'ridge'
            elif self.colonyPosition == 'e' or self.colonyPosition == 'edge':
                self.colonyPosition = 'edge'
            elif self.colonyPosition == '':
                self.colonyPosition = ' '
            else:
                print('ERROR: Bad value for colony position ('+ self.colonyPosition +'). Assuming none given. ')
                self.colonyPosition = ' '


            self.outputSegmentationPath = csvRow.get('outputSegmentationPath')
            self.outputNucSegWholeFilename = csvRow.get('outputNucSegWholeFilename')
            self.outputCellSegWholeFilename = csvRow.get('outputCellSegWholeFilename')
            self.structureSegOutputFolder = csvRow.get('structureSegOutputFolder')
            self.structureSegOutputFilename = csvRow.get('structureSegOutputFilename')

            self.outputCellSegIndex = csvRow.get('outputCellSegIndex')

            self.StructureSegmentationMethod = csvRow.get('StructureSegmentationMethod', self.StructureSegmentationMethod)

            self.structureSegContourFilename = csvRow.get('structureSegContourFilename')
            self.outputSegmentationContourPath = csvRow.get('outputSegmentationContourPath')
            self.outputNucSegContourFilename = csvRow.get('outputNucSegContourFilename')
            self.outputCellSegContourFilename = csvRow.get('outputCellSegContourFilename')

            self.cbrImageRelPath = csvRow.get('cbrImageRelPath', self.cbrImageRelPath)
            self.cbrImageLocation = csvRow.get('cbrImageLocation', self.cbrImageLocation)
            self.cbrTextureAtlasLocation = csvRow.get('cbrTextureAtlasLocation', self.cbrTextureAtlasLocation)
            self.cbrThumbnailLocation = csvRow.get('cbrThumbnailLocation', self.cbrThumbnailLocation)
            self.cbrThumbnailURL = csvRow.get('cbrThumbnailURL', self.cbrThumbnailURL)
            self.cbrThumbnailSize = csvRow.get('cbrThumbnailSize', self.cbrThumbnailSize)
            self.cbrGenerateThumbnail = csvRow.get('cbrGenerateThumbnail', self.cbrGenerateThumbnail)
            self.cbrGenerateCellImage = csvRow.get('cbrGenerateCellImage', self.cbrGenerateCellImage)
            self.cbrGenerateSegmentedImages = csvRow.get('cbrGenerateSegmentedImages', self.cbrGenerateSegmentedImages)
            self.cbrGenerateFullFieldImages = csvRow.get('cbrGenerateFullFieldImages', self.cbrGenerateFullFieldImages)
            self.cbrAddToDb = csvRow.get('cbrAddToDb', self.cbrAddToDb)
            self.cbrDatasetName = csvRow.get('cbrDatasetName', self.cbrDatasetName)
            self.cbrCellName = csvRow.get('cbrCellName', self.cbrCellName)
            self.cbrSkipStructureSegmentation = csvRow.get('cbrSkipStructureSegmentation', self.cbrSkipStructureSegmentation)

            self.dbUrl = csvRow.get('dbUrl', self.dbUrl)

