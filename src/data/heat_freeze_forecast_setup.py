from pathlib import Path

from aifs_util import (
    AIFS_ICECHUNK_PREFIX,
    AIFS_SOURCE_CREDENTIALS_PREFIX,
    BB_MLWP_VARIABLE_MAPPING,
    DEFAULT_ICECHUNK_BUCKET,
    GRAPHCAST_ICECHUNK_PREFIX,
    GRAPHCAST_SOURCE_CREDENTIALS_PREFIX,
    PANGU_ICECHUNK_PREFIX,
    PANGU_SOURCE_CREDENTIALS_PREFIX,
    InMemoryForecast,
    open_icechunk_dataset,
)  # noqa: E402
from arraylake_utils import ArraylakeForecast  # noqa: E402
from extremeweatherbench import defaults, inputs, metrics

# make the basepath - change this to your local path
basepath = Path.home() / "extreme-weather-bench-paper" / ""
basepath = str(basepath) + "/"

# setup the templates to load in the data
# Forecast Examples
cira_heat_freeze_forecast_FOURv2_IFS = inputs.KerchunkForecast(
    source="gs://extremeweatherbench/FOUR_v200_IFS.parq",
    variables=["surface_air_temperature"],
    variable_mapping={"t2": "surface_air_temperature"},
    storage_options={"remote_protocol": "s3", "remote_options": {"anon": True}},
    preprocess=defaults._preprocess_bb_cira_forecast_dataset,
    name="CIRA FOURv2 IFS",
)

cira_heat_freeze_forecast_GC_IFS = inputs.KerchunkForecast(
    source="gs://extremeweatherbench/GRAP_v100_IFS.parq",
    variables=["surface_air_temperature"],
    variable_mapping={"t2": "surface_air_temperature"},
    storage_options={"remote_protocol": "s3", "remote_options": {"anon": True}},
    preprocess=defaults._preprocess_bb_cira_forecast_dataset,
    name="CIRA GC IFS",
)

cira_heat_freeze_forecast_PANG_IFS = inputs.KerchunkForecast(
    source="gs://extremeweatherbench/PANG_v100_IFS.parq",
    variables=["surface_air_temperature"],
    variable_mapping={"t2": "surface_air_temperature"},
    storage_options={"remote_protocol": "s3", "remote_options": {"anon": True}},
    preprocess=defaults._preprocess_bb_cira_forecast_dataset,
    name="CIRA PANG IFS",
)

cira_heat_freeze_forecast_FOURv2_GFS = inputs.KerchunkForecast(
    source="gs://extremeweatherbench/FOUR_v200_GFS.parq",
    variables=["surface_air_temperature"],
    variable_mapping={"t2": "surface_air_temperature"},
    storage_options={"remote_protocol": "s3", "remote_options": {"anon": True}},
    preprocess=defaults._preprocess_bb_cira_forecast_dataset,
    name="CIRA FOURv2 GFS",
)

cira_heat_freeze_forecast_GC_GFS = inputs.KerchunkForecast(
    source="gs://extremeweatherbench/GRAP_v100_GFS.parq",
    variables=["surface_air_temperature"],
    variable_mapping={"t2": "surface_air_temperature"},
    storage_options={"remote_protocol": "s3", "remote_options": {"anon": True}},
    preprocess=defaults._preprocess_bb_cira_forecast_dataset,
    name="CIRA GC GFS",
)

cira_heat_freeze_forecast_PANG_GFS = inputs.KerchunkForecast(
    source="gs://extremeweatherbench/PANG_v100_GFS.parq",
    variables=["surface_air_temperature"],
    variable_mapping={"t2": "surface_air_temperature"},
    storage_options={"remote_protocol": "s3", "remote_options": {"anon": True}},
    preprocess=defaults._preprocess_bb_cira_forecast_dataset,
    name="CIRA PANG GFS",
)

hres_heat_freeze_forecast = inputs.ZarrForecast(
    source="gs://weatherbench2/datasets/hres/2016-2022-0012-1440x721.zarr",
    variables=["surface_air_temperature"],
    variable_mapping=inputs.HRES_metadata_variable_mapping,
    storage_options={"remote_options": {"anon": True}},
    name="ECMWF HRES",
)

bb_hres_heat_freeze_forecast = ArraylakeForecast(
    source="arraylake://brightband/ecmwf@main/forecast-archive/ewb-hres",
    variables=["surface_air_temperature"],
    variable_mapping={
        "t2m": "surface_air_temperature",
    },
    name="ECMWF HRES",
)

bb_heat_freeze_aifs_ds = open_icechunk_dataset(
    bucket=DEFAULT_ICECHUNK_BUCKET,
    prefix=AIFS_ICECHUNK_PREFIX,
    variable_mapping=BB_MLWP_VARIABLE_MAPPING,
    chunks="auto",
    source_credentials_prefix=AIFS_SOURCE_CREDENTIALS_PREFIX,
)

bb_heat_freeze_aifs_forecast = InMemoryForecast(
    bb_heat_freeze_aifs_ds,
    name="BB AIFS",
    variables=["surface_air_temperature"],
    variable_mapping=BB_MLWP_VARIABLE_MAPPING,
)

bb_heat_freeze_graphcast_ds = open_icechunk_dataset(
    bucket=DEFAULT_ICECHUNK_BUCKET,
    prefix=GRAPHCAST_ICECHUNK_PREFIX,
    variable_mapping=BB_MLWP_VARIABLE_MAPPING,
    chunks="auto",
    source_credentials_prefix=GRAPHCAST_SOURCE_CREDENTIALS_PREFIX,
)

bb_heat_freeze_graphcast_forecast = InMemoryForecast(
    bb_heat_freeze_graphcast_ds,
    name="BB Graphcast",
    variables=["surface_air_temperature"],
    variable_mapping=BB_MLWP_VARIABLE_MAPPING,
)

bb_heat_freeze_pangu_ds = open_icechunk_dataset(
    bucket=DEFAULT_ICECHUNK_BUCKET,
    prefix=PANGU_ICECHUNK_PREFIX,
    variable_mapping=BB_MLWP_VARIABLE_MAPPING,
    chunks="auto",
    source_credentials_prefix=PANGU_SOURCE_CREDENTIALS_PREFIX,
)

bb_heat_freeze_pangu_forecast = InMemoryForecast(
    bb_heat_freeze_pangu_ds,
    name="BB Pangu",
    variables=["surface_air_temperature"],
    variable_mapping=BB_MLWP_VARIABLE_MAPPING,
)

heat_metrics = [
    metrics.MaximumMeanAbsoluteError,
    metrics.RootMeanSquaredError,
    metrics.MaximumLowestMeanAbsoluteError,
    # metrics.MeanSquaredError(
    #     name="threshold_weighted_mse", interval_where_one=(313.15, np.inf)
    # ),
]

CIRA_FOURv2_HEAT_EVALUATION_OBJECTS = [
    inputs.EvaluationObject(
        event_type="heat_wave",
        metric_list=heat_metrics,
        target=defaults.ghcn_heatwave_target,
        forecast=cira_heat_freeze_forecast_FOURv2_IFS,
    ),
    inputs.EvaluationObject(
        event_type="heat_wave",
        metric_list=heat_metrics,
        target=defaults.ghcn_heatwave_target,
        forecast=cira_heat_freeze_forecast_FOURv2_GFS,
    ),
    inputs.EvaluationObject(
        event_type="heat_wave",
        metric_list=heat_metrics,
        target=defaults.era5_heatwave_target,
        forecast=cira_heat_freeze_forecast_FOURv2_IFS,
    ),
    inputs.EvaluationObject(
        event_type="heat_wave",
        metric_list=heat_metrics,
        target=defaults.era5_heatwave_target,
        forecast=cira_heat_freeze_forecast_FOURv2_GFS,
    ),
]

CIRA_GC_HEAT_EVALUATION_OBJECTS = [
    inputs.EvaluationObject(
        event_type="heat_wave",
        metric_list=heat_metrics,
        target=defaults.ghcn_heatwave_target,
        forecast=cira_heat_freeze_forecast_GC_IFS,
    ),
    inputs.EvaluationObject(
        event_type="heat_wave",
        metric_list=heat_metrics,
        target=defaults.ghcn_heatwave_target,
        forecast=cira_heat_freeze_forecast_GC_GFS,
    ),
    inputs.EvaluationObject(
        event_type="heat_wave",
        metric_list=heat_metrics,
        target=defaults.era5_heatwave_target,
        forecast=cira_heat_freeze_forecast_GC_IFS,
    ),
    inputs.EvaluationObject(
        event_type="heat_wave",
        metric_list=heat_metrics,
        target=defaults.era5_heatwave_target,
        forecast=cira_heat_freeze_forecast_GC_GFS,
    ),
]

CIRA_PANG_HEAT_EVALUATION_OBJECTS = [
    inputs.EvaluationObject(
        event_type="heat_wave",
        metric_list=heat_metrics,
        target=defaults.ghcn_heatwave_target,
        forecast=cira_heat_freeze_forecast_PANG_IFS,
    ),
    inputs.EvaluationObject(
        event_type="heat_wave",
        metric_list=heat_metrics,
        target=defaults.ghcn_heatwave_target,
        forecast=cira_heat_freeze_forecast_PANG_GFS,
    ),
    inputs.EvaluationObject(
        event_type="heat_wave",
        metric_list=heat_metrics,
        target=defaults.era5_heatwave_target,
        forecast=cira_heat_freeze_forecast_PANG_IFS,
    ),
    inputs.EvaluationObject(
        event_type="heat_wave",
        metric_list=heat_metrics,
        target=defaults.era5_heatwave_target,
        forecast=cira_heat_freeze_forecast_PANG_GFS,
    ),
]

HRES_HEAT_EVALUATION_OBJECTS = [
    inputs.EvaluationObject(
        event_type="heat_wave",
        metric_list=heat_metrics,
        target=defaults.ghcn_heatwave_target,
        forecast=hres_heat_freeze_forecast,
    ),
    inputs.EvaluationObject(
        event_type="heat_wave",
        metric_list=heat_metrics,
        target=defaults.era5_heatwave_target,
        forecast=hres_heat_freeze_forecast,
    ),
]

BB_HRES_HEAT_EVALUATION_OBJECTS = [
    inputs.EvaluationObject(
        event_type="heat_wave",
        metric_list=heat_metrics,
        target=defaults.era5_heatwave_target,
        forecast=bb_hres_heat_freeze_forecast,
    ),
    inputs.EvaluationObject(
        event_type="heat_wave",
        metric_list=heat_metrics,
        target=defaults.ghcn_heatwave_target,
        forecast=bb_hres_heat_freeze_forecast,
    ),
]

BB_HEAT_AIFS_EVALUATION_OBJECTS = [
    inputs.EvaluationObject(
        event_type="heat_wave",
        metric_list=heat_metrics,
        target=defaults.era5_heatwave_target,
        forecast=bb_heat_freeze_aifs_forecast,
    ),
    inputs.EvaluationObject(
        event_type="heat_wave",
        metric_list=heat_metrics,
        target=defaults.ghcn_heatwave_target,
        forecast=bb_heat_freeze_aifs_forecast,
    ),
]

BB_HEAT_GRAPHCAST_EVALUATION_OBJECTS = [
    inputs.EvaluationObject(
        event_type="heat_wave",
        metric_list=heat_metrics,
        target=defaults.era5_heatwave_target,
        forecast=bb_heat_freeze_graphcast_forecast,
    ),
    inputs.EvaluationObject(
        event_type="heat_wave",
        metric_list=heat_metrics,
        target=defaults.ghcn_heatwave_target,
        forecast=bb_heat_freeze_graphcast_forecast,
    ),
]

BB_HEAT_PANGU_EVALUATION_OBJECTS = [
    inputs.EvaluationObject(
        event_type="heat_wave",
        metric_list=heat_metrics,
        target=defaults.era5_heatwave_target,
        forecast=bb_heat_freeze_pangu_forecast,
    ),
    inputs.EvaluationObject(
        event_type="heat_wave",
        metric_list=heat_metrics,
        target=defaults.ghcn_heatwave_target,
        forecast=bb_heat_freeze_pangu_forecast,
    ),
]

################################################################################
# freeze metrics and evaluation objects below here
################################################################################
freeze_metrics = [
    metrics.MinimumMeanAbsoluteError,
    metrics.RootMeanSquaredError,
]

BB_HRES_FREEZE_EVALUATION_OBJECTS = [
    inputs.EvaluationObject(
        event_type="freeze",
        metric_list=freeze_metrics,
        target=defaults.era5_freeze_target,
        forecast=bb_hres_heat_freeze_forecast,
    ),
    inputs.EvaluationObject(
        event_type="freeze",
        metric_list=freeze_metrics,
        target=defaults.ghcn_freeze_target,
        forecast=bb_hres_heat_freeze_forecast,
    ),
]


FOURv2_FREEZE_EVALUATION_OBJECTS = [
    inputs.EvaluationObject(
        event_type="freeze",
        metric_list=freeze_metrics,
        target=defaults.ghcn_heatwave_target,
        forecast=cira_heat_freeze_forecast_FOURv2_IFS,
    ),
    inputs.EvaluationObject(
        event_type="freeze",
        metric_list=freeze_metrics,
        target=defaults.ghcn_heatwave_target,
        forecast=cira_heat_freeze_forecast_FOURv2_GFS,
    ),
    inputs.EvaluationObject(
        event_type="freeze",
        metric_list=freeze_metrics,
        target=defaults.era5_heatwave_target,
        forecast=cira_heat_freeze_forecast_FOURv2_IFS,
    ),
    inputs.EvaluationObject(
        event_type="freeze",
        metric_list=freeze_metrics,
        target=defaults.era5_heatwave_target,
        forecast=cira_heat_freeze_forecast_FOURv2_GFS,
    ),
]

GC_FREEZE_EVALUATION_OBJECTS = [
    inputs.EvaluationObject(
        event_type="freeze",
        metric_list=freeze_metrics,
        target=defaults.ghcn_heatwave_target,
        forecast=cira_heat_freeze_forecast_GC_IFS,
    ),
    inputs.EvaluationObject(
        event_type="freeze",
        metric_list=freeze_metrics,
        target=defaults.ghcn_heatwave_target,
        forecast=cira_heat_freeze_forecast_GC_GFS,
    ),
    inputs.EvaluationObject(
        event_type="freeze",
        metric_list=freeze_metrics,
        target=defaults.era5_heatwave_target,
        forecast=cira_heat_freeze_forecast_GC_IFS,
    ),
    inputs.EvaluationObject(
        event_type="freeze",
        metric_list=freeze_metrics,
        target=defaults.era5_heatwave_target,
        forecast=cira_heat_freeze_forecast_GC_GFS,
    ),
]

PANG_FREEZE_EVALUATION_OBJECTS = [
    inputs.EvaluationObject(
        event_type="freeze",
        metric_list=freeze_metrics,
        target=defaults.ghcn_heatwave_target,
        forecast=cira_heat_freeze_forecast_PANG_IFS,
    ),
    inputs.EvaluationObject(
        event_type="freeze",
        metric_list=freeze_metrics,
        target=defaults.ghcn_heatwave_target,
        forecast=cira_heat_freeze_forecast_PANG_GFS,
    ),
    inputs.EvaluationObject(
        event_type="freeze",
        metric_list=freeze_metrics,
        target=defaults.era5_heatwave_target,
        forecast=cira_heat_freeze_forecast_PANG_IFS,
    ),
    inputs.EvaluationObject(
        event_type="freeze",
        metric_list=freeze_metrics,
        target=defaults.era5_heatwave_target,
        forecast=cira_heat_freeze_forecast_PANG_GFS,
    ),
]

HRES_FREEZE_EVALUATION_OBJECTS = [
    inputs.EvaluationObject(
        event_type="freeze",
        metric_list=freeze_metrics,
        target=defaults.ghcn_heatwave_target,
        forecast=hres_heat_freeze_forecast,
    ),
    inputs.EvaluationObject(
        event_type="freeze",
        metric_list=freeze_metrics,
        target=defaults.era5_heatwave_target,
        forecast=hres_heat_freeze_forecast,
    ),
]

BB_FREEZE_AIFS_EVALUATION_OBJECTS = [
    inputs.EvaluationObject(
        event_type="freeze",
        metric_list=freeze_metrics,
        target=defaults.ghcn_heatwave_target,
        forecast=bb_heat_freeze_aifs_forecast,
    ),
    inputs.EvaluationObject(
        event_type="freeze",
        metric_list=freeze_metrics,
        target=defaults.era5_heatwave_target,
        forecast=bb_heat_freeze_aifs_forecast,
    ),
]

BB_FREEZE_GRAPHCAST_EVALUATION_OBJECTS = [
    inputs.EvaluationObject(
        event_type="freeze",
        metric_list=freeze_metrics,
        target=defaults.ghcn_heatwave_target,
        forecast=bb_heat_freeze_graphcast_forecast,
    ),
    inputs.EvaluationObject(
        event_type="freeze",
        metric_list=freeze_metrics,
        target=defaults.era5_heatwave_target,
        forecast=bb_heat_freeze_graphcast_forecast,
    ),
]

BB_FREEZE_PANGU_EVALUATION_OBJECTS = [
    inputs.EvaluationObject(
        event_type="freeze",
        metric_list=freeze_metrics,
        target=defaults.ghcn_heatwave_target,
        forecast=bb_heat_freeze_pangu_forecast,
    ),
    inputs.EvaluationObject(
        event_type="freeze",
        metric_list=freeze_metrics,
        target=defaults.era5_heatwave_target,
        forecast=bb_heat_freeze_pangu_forecast,
    ),
]
