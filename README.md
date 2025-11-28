# extreme-weather-bench-paper
All the code for the graphics used in the EWB paper


# notes for myself (will update the readme for global use later)

# to install EWB inside the directory for development you can do
```cd path/to/your/main-repo
mkdir -p external
git clone -b feature/new-event-types https://github.com/brightbandtech/ExtremeWeatherBench.git external/ExtremeWeatherBench
```
## then you need to add it to your uv
```# make sure you're in your project's root
cd /path/to/main-repo

# activate your venv (or create it if you haven't)
uv venv .venv
source .venv/bin/activate         # or .venv\Scripts\activate on Windows

# install the external repo in editable mode
uv pip install -e external/ExtremeWeatherBench
```
