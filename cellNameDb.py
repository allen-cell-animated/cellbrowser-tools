import csv
import re


class CellNameDatabase(object):
    # don't forget to commit cellnames.csv back into git every time it's updated for production!
    def __init__(self):
        self.filename = './data/cellnames.csv'
        with open(self.filename, 'rU') as id_file:
            id_filereader = csv.reader(id_file)
            # AICS_ID, IMAGE_NAME
            self.namedb = {rows[1]: rows[0] for rows in id_filereader}
            self.celllinedb = {}
            for key in self.namedb:
                aicsname = self.namedb[key]
                # AICS-##_###
                #  0   1   2
                inds = re.split('[_-]', aicsname)
                cell_line = int(inds[1])
                index = int(inds[2])
                # find the max.
                if cell_line in self.celllinedb:
                    if index > self.celllinedb[cell_line]:
                        self.celllinedb[cell_line] = index
                else:
                    self.celllinedb[cell_line] = index

    def get_cell_name(self, aicscelllineid, orig_name):
        if orig_name in self.namedb:
            return self.namedb[orig_name]

        # if not in db then add it.
        # get the next index for this cell line
        if aicscelllineid in self.celllinedb:
            self.celllinedb[aicscelllineid] += 1
        else:
            self.celllinedb[aicscelllineid] = 0
        index = self.celllinedb[aicscelllineid]
        # generate the name
        name = 'AICS-' + str(aicscelllineid) + '_' + str(index)
        self.namedb[orig_name] = name

        return name

    def writedb(self):
        with open(self.filename, 'w', newline='') as csv_file:
            writer = csv.writer(csv_file)
            for key, value in self.namedb.items():
                writer.writerow([value, key])
