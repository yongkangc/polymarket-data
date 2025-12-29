#!/bin/bash
# Fetch ranges 13-20 with 10 parallel chunks
source .env
export TARDIS_API_KEY
cd /home/chiayongtcac/pm/poly_data
uv run python price/fetch_remaining_10_chunks.py
