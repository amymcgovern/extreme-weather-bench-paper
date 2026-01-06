from pathlib import Path

from ewb_paper.data.aifs_util import (
    AIFS_VARIABLE_MAPPING,
    DEFAULT_ICECHUNK_BUCKET,
    DEFAULT_ICECHUNK_PREFIX,
    DEFAULT_SOURCE_CREDENTIALS_PREFIX,
    InMemoryForecast,
    open_icechunk_dataset,
)
from ewb_paper.data.arraylake_utils import (
    ArraylakeForecast,
    BB_metadata_variable_mapping,
)
from extremeweatherbench import (
    defaults,
    derived,
    inputs,
    metrics,
)

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

ds = open_icechunk_dataset(
    bucket=DEFAULT_ICECHUNK_BUCKET,
    prefix=DEFAULT_ICECHUNK_PREFIX,
    variable_mapping=AIFS_VARIABLE_MAPPING,
    chunks="auto",
    source_credentials_prefix=DEFAULT_SOURCE_CREDENTIALS_PREFIX,
)

aifs_forecast = InMemoryForecast(
    ds=ds,
    variables=[derived.CravenBrooksSignificantSevere()],
    variable_mapping=AIFS_VARIABLE_MAPPING,
    name="AIFS",
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

AIFS_SEVERE_EVALUATION_OBJECTS = [
    inputs.EvaluationObject(
        event_type="severe_convection",
        metric_list=lsr_metrics,
        target=defaults.lsr_target,
        forecast=aifs_forecast,
    ),
    inputs.EvaluationObject(
        event_type="severe_convection",
        metric_list=pph_metrics,
        target=defaults.pph_target,
        forecast=aifs_forecast,
    ),
]
