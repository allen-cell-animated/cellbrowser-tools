The Allen Institute for Cell Science microscopy images are provided in the .ome.tif format.  These files are 4D multi channel z stacks with uint16 pixel format.
Any ome.tif reader, for example ImageJ with bio-formats import, should be able to handle these.
Channel names are as follows:
DRAQ5 or CMDRP: membrane dye
EGFP: gene edited fluorescence protein
Hoechst 33258 or H3342: dna dye
TL Brightfield or Bright or Bright 100: brightfield (transmitted light) image
SEG_STRUCT: binary structure (EGFP) segmentation (filled volume)
SEG_Memb: binary membrane segmentation (filled volume).  indexed by cell number. indices will not be contiguous.
SEG_DNA: binary nucelus segmentation (filled volume). indexed by cell number. indices will not be contiguous.
CON_Memb: binary membrane contour segmentation
CON_DNA: binary nucleus contour segmentation

