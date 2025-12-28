# Poly Data Directory Reorganization Plan

## Current State Analysis

### Issues
1. ❌ **Root clutter**: 15+ loose Python scripts and markdown files at root level
2. ❌ **Mixed purposes**: Full pipeline + up/down pipeline + analysis + price fetching all mixed
3. ❌ **Unclear structure**: Hard to distinguish active vs deprecated code
4. ❌ **No clear entry points**: Multiple READMEs, unclear which to use
5. ❌ **Data organization**: data/ and other data folders (goldsky/, processed/) not clearly organized

### Current Structure
```
poly_data/
├── README.md (main project)
├── 15+ loose .py scripts at root
├── 9 markdown files at root
├── analysis/ (15+ scripts)
├── backtrader_plotting/
├── data/ (mixed outputs)
├── docs/ (4 docs)
├── goldsky/ (68GB orderFilled.csv)
├── notebooks/ (jupyter notebooks)
├── poly_utils/ (utilities)
├── price/ (13 price fetching scripts)
├── processed/ (trades.csv output)
├── update_utils/ (3 pipeline scripts)
├── updown_pipeline/ ⭐ (NEW - 9 modules)
└── .venv/, .git/, etc.
```

---

## Proposed Reorganization

### Option A: Clean Separation (RECOMMENDED)

```
poly_data/
├── README.md                          # Main overview
├── pyproject.toml                     # Project config
├── uv.lock
│
├── pipelines/                         # All pipeline code
│   ├── README.md                      # Pipeline overview
│   │
│   ├── full/                          # Original full pipeline
│   │   ├── README.md
│   │   ├── update_markets.py
│   │   ├── update_goldsky.py
│   │   ├── process_live.py
│   │   └── run_full_pipeline.py
│   │
│   └── updown/                        # Up/down market pipeline ⭐
│       ├── README.md
│       ├── __init__.py
│       ├── config.py
│       ├── checkpoint.py
│       ├── market_discovery.py
│       ├── fetch_historical_trades.py
│       ├── fetch_clob_trades.py
│       ├── integrate_binance.py
│       ├── stream_live.py
│       └── run_pipeline.py
│
├── data/                              # All data files
│   ├── README.md                      # Data documentation
│   │
│   ├── raw/                           # Raw input data
│   │   ├── goldsky/
│   │   │   └── orderFilled.csv       # 68GB blockchain data
│   │   ├── markets.csv                # Market metadata
│   │   └── binance/
│   │       └── complete_minute_data.csv
│   │
│   ├── processed/                     # Processed data (full pipeline)
│   │   └── trades.csv                 # All trades processed
│   │
│   └── updown/                        # Up/down pipeline outputs ⭐
│       ├── .checkpoints/              # Pipeline checkpoints
│       ├── markets.csv                # Discovered up/down markets
│       ├── trades_historical.csv      # Historical trades
│       └── trades_enriched.csv        # Final enriched output ⭐
│
├── analysis/                          # Analysis scripts
│   ├── README.md
│   ├── __init__.py
│   ├── btc/                           # BTC-specific analysis
│   │   ├── analyze_btc_markets.py
│   │   ├── extract_15min_trades.py
│   │   └── integrate_btc_prices.py
│   ├── patterns/                      # Pattern discovery
│   │   ├── discover_patterns.py
│   │   ├── pattern_analyzer.py
│   │   └── bucketing.py
│   └── outcomes/                      # Outcome analysis
│       ├── determine_outcomes.py
│       └── enrich_trades_with_outcomes.py
│
├── notebooks/                         # Jupyter notebooks
│   ├── README.md
│   ├── GETTING_STARTED.md
│   ├── Example 1 Trader Analysis.ipynb
│   ├── Example 2 Backtest.ipynb
│   └── scripts/
│       ├── trader_analysis.py
│       └── backtest_strategy.py
│
├── utils/                             # Shared utilities
│   ├── __init__.py
│   ├── poly_utils.py                  # Polymarket utilities
│   ├── backtrader_plotting.py         # Plotting utilities
│   └── price_fetchers/                # Price fetching tools
│       ├── fetch_binance_tardis.py
│       ├── fetch_binance_parallel.py
│       └── fill_missing_data.py
│
├── docs/                              # Documentation
│   ├── README.md
│   ├── getting-started.md
│   ├── pipelines/
│   │   ├── full-pipeline.md
│   │   └── updown-pipeline.md
│   ├── data-schema.md
│   ├── analysis-guide.md
│   └── api-reference.md
│
├── scripts/                           # One-off/utility scripts
│   ├── README.md
│   ├── fetch_wallet_goldsky.py
│   ├── verify_complete_data.py
│   ├── compare_data_completeness.py
│   └── research_updown_markets.py
│
├── archive/                           # Deprecated/old code
│   ├── README.md                      # What's archived and why
│   ├── old_price_scripts/
│   ├── old_plans/
│   └── deprecated_analysis/
│
└── .venv/, .git/, .gitignore
```

### Key Improvements

1. **✅ Clear Separation**
   - Pipelines have their own directory
   - Data organized by source/purpose
   - Analysis scripts grouped logically

2. **✅ Hierarchical Organization**
   - Each major category has subdirectories
   - Related files grouped together
   - Clear parent-child relationships

3. **✅ Discoverability**
   - README in each major directory
   - Clear naming conventions
   - Entry points obvious

4. **✅ Scalability**
   - Easy to add new pipelines
   - New analysis types can be added
   - Data organization accommodates growth

---

## Alternative: Minimal Change (Option B)

If you want minimal disruption:

```
poly_data/
├── README.md
│
├── pipelines/
│   ├── full/           # Move update_utils/ here
│   └── updown/         # Move updown_pipeline/ here
│
├── data/
│   ├── raw/            # Move goldsky/, markets.csv here
│   ├── processed/      # Keep as is
│   └── updown/         # Outputs from updown pipeline
│
├── analysis/           # Keep mostly as is
├── notebooks/          # Keep as is
├── utils/              # Merge poly_utils/, backtrader_plotting/, price/
├── scripts/            # Move loose root scripts here
└── docs/               # Keep as is
```

Less restructuring but still cleaner.

---

## Migration Plan

### Phase 1: Create New Structure (No Breaking Changes)
1. Create new directory structure
2. Copy (don't move) files to new locations
3. Update imports and paths in copied files
4. Test that new structure works

### Phase 2: Update References
1. Update all import statements
2. Update config paths
3. Update documentation
4. Update notebooks

### Phase 3: Cleanup Old Structure
1. Move old files to archive/
2. Add deprecation notices
3. Update root README with new structure
4. Delete duplicates

### Phase 4: Validation
1. Test full pipeline still works
2. Test updown pipeline works
3. Test notebooks work
4. Verify data paths correct

---

## Implementation Script

```python
# reorganize.py - Automated reorganization script

import shutil
from pathlib import Path

# Define new structure
NEW_STRUCTURE = {
    'pipelines/full': ['update_utils/'],
    'pipelines/updown': ['updown_pipeline/'],
    'data/raw/goldsky': ['goldsky/'],
    'data/processed': ['processed/'],
    'data/updown': ['data/updown*'],
    'utils': ['poly_utils/', 'backtrader_plotting/', 'price/'],
    'scripts': ['*.py at root', 'research_*.py'],
    'archive/old_plans': ['plans/', '*PLAN.md files'],
}

# Copy files to new structure (safe, no deletion)
# Update imports automatically
# Generate migration report
```

---

## Recommended Approach

**I recommend Option A (Clean Separation)** because:

1. ✅ **Future-proof**: Easy to add more pipelines
2. ✅ **Clear ownership**: Each directory has a clear purpose
3. ✅ **Professional**: Standard Python project structure
4. ✅ **Maintainable**: Easy to find and update code
5. ✅ **Collaborative**: Clear for other developers

**Migration should be done in phases** to avoid breaking existing workflows.

---

## Questions Before Implementation

1. **Which option do you prefer?**
   - Option A: Full reorganization (clean but more work)
   - Option B: Minimal change (less work but less clean)

2. **Breaking changes acceptable?**
   - Can we update import paths in existing code?
   - Are there external dependencies on current structure?

3. **Priority?**
   - Clean structure for new development?
   - Maintain compatibility with old code?
   - Balance of both?

4. **Archive strategy?**
   - Keep old scripts in archive/?
   - Delete deprecated code entirely?
   - Document what was changed?

5. **Testing requirements?**
   - Full pipeline must keep working?
   - Just updown pipeline needs to work?
   - All analysis scripts must work?

---

## Next Steps

1. **Review this plan**
2. **Answer questions above**
3. **Choose option (A or B)**
4. **Create migration script**
5. **Execute phase by phase**
6. **Update all documentation**

Let me know your preferences and I'll implement the reorganization!
