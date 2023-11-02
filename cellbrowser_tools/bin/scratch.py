import bioio


source = "\\\\allen\\aics\\assay-dev\\MicroscopyData\\Leigh\\20230214\\Timelapse_5min_50ms_50ms-04(2).czi"
reader = bioio.BioImage(source)
img = reader.get_image_dask_data("ZYX", T=0, C=0).max(axis=0)
img = img.compute()
