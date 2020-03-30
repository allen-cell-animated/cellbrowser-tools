from enum import Enum

IMAGES_DIR = "Cell-Viewer_Data"
THUMBNAILS_DIR = "Cell-Viewer_Thumbnails"
ATLAS_DIR = "Cell-Viewer_Thumbnails"

FEATURE_DATA_FILENAME = "cell-feature-analysis.json"
CELL_LINE_DATA_FILENAME = "cell-line-def.json"
FILE_LIST_FILENAME = "cellviewer-files.csv"

ERROR_FOVS_FILENAME = "errorFovs.txt"
CHANNEL_NAMES_FILENAME = "allChannelNames.txt"


# the expected column names returned from labkey
class DataField(Enum):
    CellId = "CellId"
    CellIndex = "CellIndex"
    SourceFileId = "SourceFileId"
    SourceFilename = "SourceFilename"
    SourceReadPath = "SourceReadPath"
    FOVId = "FOVId"
    NucleusSegmentationFileId = "NucleusSegmentationFileId"
    NucleusSegmentationFilename = "NucleusSegmentationFilename"
    NucleusSegmentationReadPath = "NucleusSegmentationReadPath"
    NucleusContourFileId = "NucleusContourFileId"
    NucleusContourFilename = "NucleusContourFilename"
    NucleusContourReadPath = "NucleusContourReadPath"
    MembraneSegmentationFileId = "MembraneSegmentationFileId"
    MembraneSegmentationFilename = "MembraneSegmentationFilename"
    MembraneSegmentationReadPath = "MembraneSegmentationReadPath"
    MembraneContourFileId = "MembraneContourFileId"
    MembraneContourFilename = "MembraneContourFilename"
    MembraneContourReadPath = "MembraneContourReadPath"
    StructureSegmentationFileId = "StructureSegmentationFileId"
    StructureSegmentationFilename = "StructureSegmentationFilename"
    StructureSegmentationReadPath = "StructureSegmentationReadPath"
    StructureContourFileId = "StructureContourFileId"
    StructureContourFilename = "StructureContourFilename"
    StructureContourReadPath = "StructureContourReadPath"
    StructureSegmentationAlgorithm = "StructureSegmentationAlgorithm"
    StructureSegmentationAlgorithmVersion = "StructureSegmentationAlgorithmVersion"
    ChannelNumber405 = "ChannelNumber405"
    ChannelNumber638 = "ChannelNumber638"
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
    StructEducationName = "StructEducationName"
    Protein = "Protein"
    ProteinDisplayName = "ProteinDisplayName"
    Gene = "Gene"
    ColonyPosition = "ColonyPosition"
    DataSetId = "DataSetId"
