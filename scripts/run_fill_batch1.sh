#!/bin/bash
# Fill ranges 13-19 with MAX_PARALLEL=4
source .env
export TARDIS_API_KEY
cd /home/chiayongtcac/pm/poly_data
uv run python price/fill_remaining_batch1.py
