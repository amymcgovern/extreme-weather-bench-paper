# setup all the imports
from dataclasses import dataclass

import pandas as pd
import seaborn as sns
import xarray as xr
from arraylake import Client
from extremeweatherbench import (
    cases,
    defaults,
    derived,
    evaluate,
    inputs,
    metrics,
)

sns.set_theme(style="whitegrid")
from pathlib import Path  # noqa: E402

# make the basepath - change this to your local path
basepath = Path.home() / "extreme-weather-bench-paper" / ""
basepath = str(basepath) + "/"

BB_metadata_variable_mapping = {
    "isobaricInhPa": "level",
    "t2m": "surface_air_temperature",
    "t": "air_temperature",
    "q": "specific_humidity",
    "u": "eastward_wind",
    "v": "northward_wind",
    "p": "air_pressure",
    "z": "geopotential",
    "r": "relative_humidity",
    "u10": "surface_eastward_wind",
    "v10": "surface_northward_wind",
    "u100": "100m_eastward_wind",
    "v100": "100m_northward_wind",
    "msl": "air_pressure_at_mean_sea_level",
}


@dataclass
class ArraylakeForecast(inputs.ForecastBase):
    prefetch: bool = True

    def _prefetch_data(self):
        client = Client()
        repo = client.get_repo(f"{self.org_name}/{self.repo_name}")
        session = repo.readonly_session(self.branch_name)
        self.ds = xr.open_zarr(session.store, group=self.group_name)
        self.ds = self.ds.assign_coords(
            {"lead_time": self.ds.lead_time.astype("timedelta64[h]")}
        )

    def __post_init__(self):
        # source should be like arraylake://org_name/repo_name@branch_name/group/name/goes/here
        if not self.source.startswith("arraylake://"):
            raise ValueError("source must start with arraylake://")
        bits = self.source.split("://")[1].split("/")
        self.org_name = bits[0]
        if "@" not in bits[1]:
            self.branch_name = "main"
            self.repo_name = bits[1]
        else:
            self.repo_name, self.branch_name = bits[1].split("@")
        self.group_name = "/".join(bits[2:])
        if self.prefetch:
            self._prefetch_data()

    def _open_data_from_source(self) -> xr.Dataset:
        if self.ds is None:
            self._prefetch_data()
        return self.ds


# setup the templates to load in the data

cira_severe_convection_forecast_FOURV2_GFS = inputs.KerchunkForecast(
    source="gs://extremeweatherbench/FOUR_v200_GFS.parq",
    variables=[derived.CravenBrooksSignificantSevere()],
    variable_mapping=inputs.CIRA_metadata_variable_mapping,
    storage_options={"remote_protocol": "s3", "remote_options": {"anon": True}},
    name="CIRA FOURv2 GFS",
    preprocess=defaults._preprocess_bb_severe_cira_forecast_dataset,
)

cira_severe_convection_forecast_GC_GFS = inputs.KerchunkForecast(
    source="gs://extremeweatherbench/GRAP_v100_GFS.parq",
    variables=[derived.CravenBrooksSignificantSevere()],
    variable_mapping=inputs.CIRA_metadata_variable_mapping,
    storage_options={"remote_protocol": "s3", "remote_options": {"anon": True}},
    name="CIRA GC GFS",
    preprocess=defaults._preprocess_bb_severe_cira_forecast_dataset,
)

cira_severe_convection_forecast_PANG_GFS = inputs.KerchunkForecast(
    source="gs://extremeweatherbench/PANG_v100_GFS.parq",
    variables=[derived.CravenBrooksSignificantSevere()],
    variable_mapping=inputs.CIRA_metadata_variable_mapping,
    storage_options={"remote_protocol": "s3", "remote_options": {"anon": True}},
    name="CIRA PANG GFS",
    preprocess=defaults._preprocess_bb_severe_cira_forecast_dataset,
)

hres_severe_forecast = inputs.ZarrForecast(
    source="gs://weatherbench2/datasets/hres/2016-2022-0012-1440x721.zarr",
    variables=[derived.CravenBrooksSignificantSevere()],
    variable_mapping=inputs.HRES_metadata_variable_mapping,
    storage_options={"remote_options": {"anon": True}},
    name="ECMWF HRES",
)

bb_hres_forecast = ArraylakeForecast(
    source="arraylake://brightband/ecmwf@main/forecast-archive/ewb-hres",
    variables=[derived.CravenBrooksSignificantSevere()],
    storage_options={"remote_options": {"anon": True}},
    name="ECMWF HRES",
    variable_mapping=BB_metadata_variable_mapping,
)


# Define threshold metrics
pph_metrics = [
    metrics.ThresholdMetric(
        metrics=[
            metrics.CriticalSuccessIndex(),
            metrics.FalseAlarmRatio(),
        ],
        forecast_threshold=15000,
        target_threshold=0.01,
    ),
    metrics.EarlySignal(threshold=15000),
]

# Define LSR metrics
lsr_metrics = [
    metrics.ThresholdMetric(
        metrics=[
            metrics.TruePositives(),
            metrics.FalseNegatives(),
        ],
        forecast_threshold=15000,
        target_threshold=0.5,
    )
]

FOURv2_SEVERE_EVALUATION_OBJECTS = [
    inputs.EvaluationObject(
        event_type="severe_convection",
        metric_list=lsr_metrics,
        target=defaults.lsr_target,
        forecast=cira_severe_convection_forecast_FOURV2_GFS,
    ),
    inputs.EvaluationObject(
        event_type="severe_convection",
        metric_list=pph_metrics,
        target=defaults.pph_target,
        forecast=cira_severe_convection_forecast_FOURV2_GFS,
    ),
]

GC_SEVERE_EVALUATION_OBJECTS = [
    inputs.EvaluationObject(
        event_type="severe_convection",
        metric_list=lsr_metrics,
        target=defaults.lsr_target,
        forecast=cira_severe_convection_forecast_GC_GFS,
    ),
    inputs.EvaluationObject(
        event_type="severe_convection",
        metric_list=pph_metrics,
        target=defaults.pph_target,
        forecast=cira_severe_convection_forecast_GC_GFS,
    ),
]

PANG_SEVERE_EVALUATION_OBJECTS = [
    inputs.EvaluationObject(
        event_type="severe_convection",
        metric_list=lsr_metrics,
        target=defaults.lsr_target,
        forecast=cira_severe_convection_forecast_PANG_GFS,
    ),
    inputs.EvaluationObject(
        event_type="severe_convection",
        metric_list=pph_metrics,
        target=defaults.pph_target,
        forecast=cira_severe_convection_forecast_PANG_GFS,
    ),
]

HRES_SEVERE_EVALUATION_OBJECTS = [
    inputs.EvaluationObject(
        event_type="severe_convection",
        metric_list=lsr_metrics,
        target=defaults.lsr_target,
        forecast=hres_severe_forecast,
    ),
    inputs.EvaluationObject(
        event_type="severe_convection",
        metric_list=pph_metrics,
        target=defaults.pph_target,
        forecast=hres_severe_forecast,
    ),
]

BB_HRES_SEVERE_EVALUATION_OBJECTS = [
    inputs.EvaluationObject(
        event_type="severe_convection",
        metric_list=lsr_metrics,
        target=defaults.lsr_target,
        forecast=bb_hres_forecast,
    ),
    inputs.EvaluationObject(
        event_type="severe_convection",
        metric_list=pph_metrics,
        target=defaults.pph_target,
        forecast=bb_hres_forecast,
    ),
]

# load in all of the events in the yaml file
ewb_cases = cases.load_ewb_events_yaml_into_case_collection()
ewb_cases = ewb_cases.select_cases("event_type", "severe_convection")


ewb_hres = evaluate.ExtremeWeatherBench(ewb_cases, HRES_SEVERE_EVALUATION_OBJECTS)
ewb_hres_bb = evaluate.ExtremeWeatherBench(ewb_cases, BB_HRES_SEVERE_EVALUATION_OBJECTS)
ewb_fourv2 = evaluate.ExtremeWeatherBench(ewb_cases, FOURv2_SEVERE_EVALUATION_OBJECTS)
ewb_gc = evaluate.ExtremeWeatherBench(ewb_cases, GC_SEVERE_EVALUATION_OBJECTS)
ewb_pang = evaluate.ExtremeWeatherBench(ewb_cases, PANG_SEVERE_EVALUATION_OBJECTS)


parallel_config = {"backend": "loky", "n_jobs": 24}

print("running HRES part 1")
hres_results1 = ewb_hres.run(parallel_config=parallel_config)
print("running HRES part 2")
hres_results2 = ewb_hres_bb.run(parallel_config=parallel_config)
print("concatenating the results")
hres_results = pd.concat([hres_results1, hres_results2])
print("saving the results")
hres_results.to_pickle(basepath + "saved_data/hres_severe_results.pkl")

# fourv2_results = ewb_fourv2.run(parallel_config=parallel_config)
# gc_results = ewb_gc.run(parallel_config=parallel_config)
# pang_results = ewb_pang.run(parallel_config=parallel_config)

# fourv2_results.to_pickle(basepath + "saved_data/fourv2_severe_results.pkl")
# gc_results.to_pickle(basepath + "saved_data/gc_severe_results.pkl")
# pang_results.to_pickle(basepath + "saved_data/pang_severe_results.pkl")
