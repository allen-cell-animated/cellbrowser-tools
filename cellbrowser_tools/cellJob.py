class CellJob(object):
    def __init__(self, csvRows):
        self.cells = csvRows
        # initialize some defaults
        self.cbrThumbnailSize = 128
        # root output directory for thumbnail images
        self.cbrThumbnailLocation = ""
        # root output directory for ome-tiff images
        self.cbrImageLocation = ""
        # root output directory for volume-viewer texture atlases
        self.cbrTextureAtlasLocation = ""
