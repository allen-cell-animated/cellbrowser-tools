from cellbrowser_tools.dataHandoffUtils import collect_csv_data_rows

import pandas as pd

dataset = collect_csv_data_rows()
dataset = dataset.drop_duplicates(["FOVId"])
# start to build a new csv
out = dataset[
    [
        "FOVId",
        "SourceFilename",
        "CellLine",
        "Structure",
        "Gene",
        "ColonyPosition",
        "InstrumentId",
        "PlateId",
        "WellName",
    ]
].copy()
# required:
# file_id, file_name, file_path, file_size, thumbnail, uploaded
out = out.rename(columns={"FOVId": "file_id", "SourceFilename": "file_name"})


def fov_id_to_path(fov_id):
    return f"https://animatedcell-test-data.s3.us-west-2.amazonaws.com/variance/{fov_id}.zarr"


out["file_path"] = out["file_id"].apply(lambda x: fov_id_to_path(x))


# Define a custom function to combine columns
def make_thumbnail(row):
    return f"https://s3-us-west-2.amazonaws.com/bisque.allencell.org/v2.0.0/Cell-Viewer_Thumbnails/{ row['CellLine'] }/{ row['CellLine'] }_{ row['file_id'] }.png"


# Apply the custom function to create a new column 'thumbnail'
out["thumbnail"] = out.apply(make_thumbnail, axis=1)
out["file_size"] = [0 for i in range(len(out))]
out["uploaded"] = [0 for i in range(len(out))]
# fovid, name, url, anything, thumburl, any date stamp
out.to_csv("test.csv")
