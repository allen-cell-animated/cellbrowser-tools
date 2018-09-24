
class CellJob(object):
    def __init__(self, csvRow):
        for key in csvRow:
            setattr(self, key, csvRow[key])

