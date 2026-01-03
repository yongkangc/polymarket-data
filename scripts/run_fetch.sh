#!/bin/bash
source .env
export TARDIS_API_KEY
cd /home/chiayongtcac/pm/poly_data
uv run python price/fetch_binance_parallel.py
