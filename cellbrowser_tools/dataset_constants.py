from enum import Enum

IMAGES_DIR = "Cell-Viewer_Data"
THUMBNAILS_DIR = "Cell-Viewer_Thumbnails"
ATLAS_DIR = "Cell-Viewer_Thumbnails"

FEATURE_DATA_FILENAME_OLD = "cell-feature-analysis.json"
CELL_LINE_DATA_FILENAME = "cell-line-def.json"
FILE_LIST_FILENAME = "cellviewer-files.csv"
DATASET_JSON_FILENAME = "dataset_cached.json"

ERROR_FOVS_FILENAME = "errorFovs.txt"
CHANNEL_NAMES_FILENAME = "allChannelNames.txt"

# files for new data set specification
FEATURE_DATA_FILENAME = "features.json"
CELL_LINE_DEF_FILENAME = "cell_line_def.json"
FEATURE_DEF_FILENAME = "feature_def.json"
DATASET_FILENAME = "dataset_def.json"


# the expected column names returned from labkey
class DataField(str, Enum):
    AlignedImageReadPath = "AlignedImageReadPath"
    CellId = "CellId"
    CellIndex = "CellIndex"
    SourceFileId = "SourceFileId"
    SourceFilename = "SourceFilename"
    SourceReadPath = "SourceReadPath"
    FOVId = "FOVId"
    NucleusSegmentationFileId = "NucleusSegmentationFileId"
    NucleusSegmentationFilename = "NucleusSegmentationFilename"
    NucleusSegmentationReadPath = "NucleusSegmentationReadPath"
    NucleusSegmentationChannelIndex = "NucleusSegmentationChannelIndex"
    NucleusContourFileId = "NucleusContourFileId"
    NucleusContourFilename = "NucleusContourFilename"
    NucleusContourReadPath = "NucleusContourReadPath"
    NucleusContourChannelIndex = "NucleusContourChannelIndex"
    MembraneSegmentationFileId = "MembraneSegmentationFileId"
    MembraneSegmentationFilename = "MembraneSegmentationFilename"
    MembraneSegmentationReadPath = "MembraneSegmentationReadPath"
    MembraneSegmentationChannelIndex = "MembraneSegmentationChannelIndex"
    MembraneContourFileId = "MembraneContourFileId"
    MembraneContourFilename = "MembraneContourFilename"
    MembraneContourReadPath = "MembraneContourReadPath"
    MembraneContourChannelIndex = "MembraneContourChannelIndex"
    StructureSegmentationFileId = "StructureSegmentationFileId"
    StructureSegmentationFilename = "StructureSegmentationFilename"
    StructureSegmentationReadPath = "StructureSegmentationReadPath"
    StructureContourFileId = "StructureContourFileId"
    StructureContourFilename = "StructureContourFilename"
    StructureContourReadPath = "StructureContourReadPath"
    StructureSegmentationAlgorithm = "StructureSegmentationAlgorithm"
    StructureSegmentationAlgorithmVersion = "StructureSegmentationAlgorithmVersion"
    ChannelNumber405 = "ChannelNumber405"  # dna
    # ChannelNumber488 = "ChannelNumber488" # possible struct
    # ChannelNumber561 = "ChannelNumber561" # possible struct
    ChannelNumber638 = "ChannelNumber638"  # membrane
    ChannelNumberStruct = "ChannelNumberStruct"
    ChannelNumberBrightfield = "ChannelNumberBrightfield"
    NucMembSegmentationAlgorithm = "NucMembSegmentationAlgorithm"
    NucMembSegmentationAlgorithmVersion = "NucMembSegmentationAlgorithmVersion"
    RunId = "RunId"
    PixelScaleX = "PixelScaleX"
    PixelScaleY = "PixelScaleY"
    PixelScaleZ = "PixelScaleZ"
    Objective = "Objective"
    InstrumentId = "InstrumentId"
    WellId = "WellId"
    Row = "Row"
    Col = "Col"
    WellName = "WellName"
    PlateId = "PlateId"
    WorkflowId = "WorkflowId"
    Workflow = "Workflow"
    CellPopulationId = "CellPopulationId"
    CellLineId = "CellLineId"
    CellLine = "CellLine"
    Clone = "Clone"
    Passage = "Passage"
    StructureId = "StructureId"
    Structure = "Structure"
    StructureDisplayName = "StructureDisplayName"
    StructureShortName = "StructureShortName"
    Protein = "Protein"
    ProteinDisplayName = "ProteinDisplayName"
    Gene = "Gene"
    ColonyPosition = "ColonyPosition"
    DataSetId = "DataSetId"


class AugmentedDataField(str, Enum):
    IsMitotic = "IsMitotic"
    MitoticState = "MitoticState"
