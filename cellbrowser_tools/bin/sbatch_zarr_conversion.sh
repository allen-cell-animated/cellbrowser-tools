#!/bin/bash
#
#SBATCH --output=%x_%j.out
#SBATCH --error=%x_%j.err
#SBATCH --mem=32G
#SBATCH --cpus-per-task=16

module add anaconda3
source activate
conda activate cellbrowser-tools
python make_zarr_timeseries_segs.py

