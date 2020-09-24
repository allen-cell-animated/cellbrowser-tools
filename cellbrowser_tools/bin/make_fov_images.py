from cellbrowser_tools import fov_processing, cellJob

import lkaccess
import lkaccess.contexts
from lkaccess import LabKey
import pandas as pd

lk = LabKey(server_context=lkaccess.contexts.PROD)

print("REQUESTING DATA HANDOFF")
# lkdatarows = lk.dataset.get_fov_dataset(126)
lkdatarows = lk.dataset.get_cell_dataset(126)
df_data_handoff = pd.DataFrame(lkdatarows)

# Merge Aligned and Source read path columns into SourceReadPath
df_data_handoff["SourceReadPath"] = df_data_handoff[
    "AlignedImageReadPath"
].combine_first(df_data_handoff["SourceReadPath"])

# group by fov id
data_grouped = df_data_handoff.groupby("FOVId")
total_jobs = len(data_grouped)
# log.info('ABOUT TO CREATE ' + str(total_jobs) + ' JOBS')
groups = []
for index, (fovid, group) in enumerate(data_grouped):
    print(fovid)
    g = group.to_dict(orient="records")
    # jsut select the first one, we only need one representative per FOV
    g = g[0]
    g = cellJob.CellJob([g])
    g.cbrThumbnailSize = 128
    g.cbrThumbnailLocation = "//allen/aics/animated-cell/Dan/fileserver/DS126/"
    g.cbrImageLocation = "//allen/aics/animated-cell/Dan/fileserver/DS126/"
    g.cbrTextureAtlasLocation = "//allen/aics/animated-cell/Dan/fileserver/DS126/"

    processor = fov_processing.ImageProcessor(g)
    processor.generate_and_save(do_segmented_cells=False)
