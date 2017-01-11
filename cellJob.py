
class CellJob(object):
    def __init__(self, csvRow):
        self.DeliveryDate = '00/00/0000'
        self.Version = '0.0'
        self.inputFolder = ''
        self.inputFilename = ''
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
        self.cbrImageLocation = './images'
        self.cbrThumbnailLocation = './images'
        self.cbrThumbnailURL = 'file:///images'
        self.cbrThumbnailSize = 128
        # processing
        self.cbrGenerateThumbnail = False
        self.cbrGenerateCellImage = False
        self.cbrAddToDb = False
        if csvRow is not None:
            self.DeliveryDate = csvRow.get('DeliveryDate')
            self.Version = csvRow.get('Version')
            self.inputFolder = csvRow.get('inputFolder')
            self.inputFilename = csvRow.get('inputFilename')
            self.xyPixelSize = float(csvRow.get('xyPixelSize', 0))
            self.zPixelSize = float(csvRow.get('zPixelSize', 0))
            self.memChannel = int(float(csvRow.get('memChannel', 0)))
            self.nucChannel = int(float(csvRow.get('nucChannel', 0)))
            self.structureChannel = int(float(csvRow.get('structureChannel', 0)))
            self.structureProteinName = csvRow.get('structureProteinName')
            self.lightChannel = int(float(csvRow.get('lightChannel', 0)))
            self.timePoint = int(float(csvRow.get('timePoint', 0)))
            self.outputSegmentationPath = csvRow.get('outputSegmentationPath')
            self.outputNucSegWholeFilename = csvRow.get('outputNucSegWholeFilename')
            self.outputCellSegWholeFilename = csvRow.get('outputCellSegWholeFilename')
            self.structureSegOutputFolder = csvRow.get('structureSegOutputFolder')
            self.structureSegOutputFilename = csvRow.get('structureSegOutputFilename')
            self.cbrImageLocation = csvRow.get('cbrImageLocation', self.cbrImageLocation)
            self.cbrThumbnailLocation = csvRow.get('cbrThumbnailLocation', self.cbrThumbnailLocation)
            self.cbrThumbnailURL = csvRow.get('cbrThumbnailURL', self.cbrThumbnailURL)
            self.cbrThumbnailSize = csvRow.get('cbrThumbnailSize', self.cbrThumbnailSize)
            self.cbrGenerateThumbnail = csvRow.get('cbrGenerateThumbnail', self.cbrGenerateThumbnail)
            self.cbrGenerateCellImage = csvRow.get('cbrGenerateCellImage', self.cbrGenerateCellImage)
            self.cbrAddToDb = csvRow.get('cbrAddToDb', self.cbrAddToDb)
