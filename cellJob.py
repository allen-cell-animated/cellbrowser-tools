
class CellJob(object):
    def __init__(self, csvRow):
        self.DeliveryDate = '00/00/0000'
        self.Version = '0.0'
        self.inputFolder = ''
        self.inputFilename = ''
        self.inputFileRow = 0

        self.xyPixelSize = 0.0
        self.zPixelSize = 0.0

        self.memChannel = 0
        self.nucChannel = 0
        self.structureChannel = 0
        self.structureProteinName = ''
        self.lightChannel = 0
        self.timePoint = 0

        self.outputSegmentationPath = ''
        self.outputNucSegWholeFilename = ''
        self.outputCellSegWholeFilename = ''
        self.structureSegOutputFolder = ''
        self.structureSegOutputFilename = ''

        self.StructureSegmentationMethod = ''

        self.outputSegmentationContourPath = ''
        self.structureSegContourFilename = ''
        self.outputNucSegContourFilename = ''
        self.outputCellSegContourFilename = ''

        self.cbrImageLocation = './images'
        self.cbrThumbnailLocation = './images'
        self.cbrThumbnailURL = 'file:///images'
        self.cbrThumbnailSize = 128

        self.cbrDataRoot = '/data/aics/software_it/danielt/images/AICS/bisque/'
        self.cbrThumbnailRoot = '/data/aics/software_it/danielt/demos/bisque/thumbnails/'
        self.cbrThumbnailWebRoot = 'http://stg-aics.corp.alleninstitute.org/danielt_demos/bisque/thumbnails/'

        self.cbrDatasetName = ''
        self.cbrCellName = ''

        # processing

        self.cbrGenerateThumbnail = False
        self.cbrGenerateCellImage = False
        self.cbrAddToDb = False
        self.cbrGenerateSegmentedImages = True
        self.cbrGenerateFullFieldImages = True
        self.cbrParseError = False
        if csvRow is not None:
            self.cbrParseError = csvRow.get('cbrParseError', False)
            self.DeliveryDate = csvRow.get('DeliveryDate')
            self.Version = csvRow.get('Version')
            self.inputFolder = csvRow.get('inputFolder')
            self.inputFilename = csvRow.get('inputFilename')
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

            self.outputSegmentationPath = csvRow.get('outputSegmentationPath')
            self.outputNucSegWholeFilename = csvRow.get('outputNucSegWholeFilename')
            self.outputCellSegWholeFilename = csvRow.get('outputCellSegWholeFilename')
            self.structureSegOutputFolder = csvRow.get('structureSegOutputFolder')
            self.structureSegOutputFilename = csvRow.get('structureSegOutputFilename')

            self.StructureSegmentationMethod = csvRow.get('StructureSegmentationMethod', self.StructureSegmentationMethod)

            self.structureSegContourFilename = csvRow.get('structureSegContourFilename')
            self.outputSegmentationContourPath = csvRow.get('outputSegmentationContourPath')
            self.outputNucSegContourFilename = csvRow.get('outputNucSegContourFilename')
            self.outputCellSegContourFilename = csvRow.get('outputCellSegContourFilename')

            self.cbrImageLocation = csvRow.get('cbrImageLocation', self.cbrImageLocation)
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
