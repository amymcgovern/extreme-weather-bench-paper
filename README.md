# extreme-weather-bench-paper

Analysis code and figure generation for the Extreme Weather Bench (EWB) paper.

## Installation

This project uses [uv](https://github.com/astral-sh/uv) for fast, reliable dependency management.

### Prerequisites

1. Install uv:
   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

2. Python 3.11 or later

### Quick Start

1. Clone the repository:
   ```bash
   git clone https://github.com/brightbandtech/extreme-weather-bench-paper.git
   cd extreme-weather-bench-paper
   ```

2. Create virtual environment and install dependencies:
   ```bash
   uv venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   uv pip install -e .
   ```

3. Install the extremeweatherbench library (required):
   ```bash
   # Option A: Clone and install locally (recommended for development)
   mkdir -p external
   git clone -b feature/new-event-types https://github.com/brightbandtech/extremeweatherbench.git external/ExtremeWeatherBench
   uv pip install -e external/ExtremeWeatherBench

   # Option B: Install directly from GitHub (if you have access)
   uv pip install "git+https://github.com/brightbandtech/extremeweatherbench.git@feature/new-event-types"
   ```

**Note:** The extremeweatherbench library is installed separately to allow for local development and to handle private repository access.

## Local Development with extremeweatherbench

If you need to modify the `extremeweatherbench` library while working on this project:

1. Clone the extremeweatherbench repository:
   ```bash
   mkdir -p external
   git clone -b feature/new-event-types https://github.com/brightbandtech/extremeweatherbench.git external/ExtremeWeatherBench
   ```

2. Install in editable mode:
   ```bash
   uv pip uninstall extremeweatherbench  # Remove the GitHub version
   uv pip install -e external/ExtremeWeatherBench
   ```

3. To switch back to the GitHub version:
   ```bash
   uv pip uninstall extremeweatherbench
   uv pip install "extremeweatherbench @ git+https://github.com/brightbandtech/extremeweatherbench@feature/new-event-types"
   ```

## Configuration

Create a `config.toml` file in the project root (optional):

```toml
[paths]
basepath = "~/extreme-weather-bench-paper"

[parallel]
backend = "loky"
n_jobs = 32
```

If no config file is provided, defaults will be used. You can also set the environment variable `EWB_PAPER_BASEPATH` to customize the data directory.

## Usage

### Running Evaluations

Use uv to run the analysis scripts:

```bash
# Heat wave analysis
uv run heat-analysis --run_hres --run_bb_aifs

# Freeze event analysis
uv run freeze-analysis --run_hres --run_cira_pangu

# Severe convection analysis
uv run severe-analysis --run_hres --run_aifs

# Tropical cyclone analysis
uv run tc-analysis

# Atmospheric river analysis
uv run ar-analysis
```

Each script supports multiple model flags:
- `--run_hres` - ECMWF HRES forecast
- `--run_cira_fourv2` - CIRA FOURv2 model
- `--run_cira_gc` - CIRA GraphCast model
- `--run_cira_pangu` - CIRA Pangu-Weather model
- `--run_bb_aifs` - BrightBand AIFS model
- `--run_bb_graphcast` - BrightBand GraphCast model
- `--run_bb_pangu` - BrightBand Pangu-Weather model

### Pre-computing Plot Data

For faster figure generation, pre-compute derived variables:

```bash
# Atmospheric river graphics
uv run compute-ar-plots

# Severe convection CBSS/PPH data
uv run compute-cbss-plots --run_hres --run_aifs
```

### Generating Figures

Open the Jupyter notebooks in the `notebooks/` directory:

```bash
jupyter lab notebooks/
```

Notebooks available:
- `figure1.ipynb` - Main overview figure
- `heat_figures.ipynb` - Heat wave analysis
- `freeze_figures.ipynb` - Freeze event analysis
- `severe_figures.ipynb` - Severe convection results
- `tc_figures.ipynb` - Tropical cyclone tracks
- `ar_figures.ipynb` - Atmospheric river cases
- And more...

## Project Structure

```
extreme-weather-bench-paper/
├── pyproject.toml          # Project configuration and dependencies
├── src/ewb_paper/          # Main Python package
│   ├── config.py           # Configuration management
│   ├── data/               # Data processing and evaluation
│   └── plots/              # Plotting utilities
├── notebooks/              # Jupyter notebooks for figures
└── saved_data/             # Cached evaluation results (not in git)
```

## Dependencies

Main dependencies:
- **extremeweatherbench** - Core evaluation library
- xarray, pandas, numpy - Data manipulation
- matplotlib, cartopy, seaborn - Visualization
- icechunk, arraylake - Cloud storage access
- joblib - Parallel processing

See `pyproject.toml` for complete dependency list.

## License

MIT License

## Citation

[Add paper citation when published]
