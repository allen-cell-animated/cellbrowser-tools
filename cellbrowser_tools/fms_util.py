from .dataHandoffUtils import normalize_path

from aicsfiles import fms, FileLevelMetadataKeys

def fms_id_to_path(fms_id:str)->str:
    # info = {"fmsid": fms_id}
    annotations = {FileLevelMetadataKeys.FILE_ID.value: fms_id}
    fms_file = list(
        fms.find(
            annotations=annotations,
            limit=1,
        )
    )[0]
    path = fms_file.path
    path = normalize_path(path)
    return path
