# extreme-weather-bench-paper
All the code for the graphics used in the EWB paper


# notes for myself (will update the readme for global use later)

## Install this package in editable mode

To import classes from `src.data` and `src.plots` modules, install this package in editable mode:

```bash
# Make sure you're in the project root
cd /path/to/extreme-weather-bench-paper

# Activate your venv (or create it if you haven't)
uv venv .venv
source .venv/bin/activate         # or .venv\Scripts\activate on Windows

# Install this package in editable mode
pip install -e .
# or if using uv:
uv pip install -e .
```

After installation, you can import from the package:
```python
from src.data.severe_forecast_setup import aifs_forecast, hres_severe_forecast
from src.data.heat_freeze_forecast_setup import heat_freeze_forecast_setup
from src.plots.plotting_utils import convert_longitude_for_plotting
```

## to install EWB inside the directory for development you can do
```
cd path/to/your/main-repo
mkdir -p external
git clone -b feature/new-event-types https://github.com/brightbandtech/ExtremeWeatherBench.git external/ExtremeWeatherBench
```
## then you need to add it to your uv
```
# make sure you're in your project's root
cd /path/to/main-repo

# activate your venv (or create it if you haven't)
uv venv .venv
source .venv/bin/activate         # or .venv\Scripts\activate on Windows

# install the external repo in editable mode
uv pip install -e external/ExtremeWeatherBench
```
