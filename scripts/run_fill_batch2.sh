#!/bin/bash
# Fill range 20 (the big one) with dedicated single fetch
source .env
export TARDIS_API_KEY
cd /home/chiayongtcac/pm/poly_data
uv run python price/fill_remaining_batch2.py
