from cellbrowser_tools.dataHandoffUtils import collect_csv_data_rows

import pandas as pd

dataset = collect_csv_data_rows()
dataset = dataset.drop_duplicates(["FOVId"])
# start to build a new csv
out = dataset[["FOVId", "SourceFilename", "CellLine", "Structure", "Gene", "ColonyPosition", "InstrumentId", "WellId", "PlateId", "WellName" ]].copy()
# required:
# file_id, file_name, file_path, file_size, thumbnail, uploaded
# fovid, name, url, anything, thumburl, any date stamp
out.to_csv("test.csv")
