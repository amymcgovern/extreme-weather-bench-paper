# setup all the imports
import matplotlib.font_manager

flist = matplotlib.font_manager.get_font_names()
import pickle
from pathlib import Path  # noqa: E402

from extremeweatherbench import cases, defaults, derived, evaluate, inputs

# make the basepath - change this to your local path
basepath = Path.home() / "extreme-weather-bench-paper" / ""
basepath = str(basepath) + "/"

# ugly hack to load in our plotting scripts
# import sys  # noqa: E402

# sys.path.append(basepath + "/docs/notebooks/")

# load in all of the events in the yaml file
print("loading in the events yaml file")
ewb_cases = cases.load_ewb_events_yaml_into_case_collection()

# downselect to only the severe cases
ewb_cases = ewb_cases.select_cases("event_type", "severe_convection")

# build out all of the expected data to evalate the case (we need this so we can plot
# the LSR reports)
case_operators = cases.build_case_operators(
    ewb_cases, defaults.get_brightband_evaluation_objects()
)
# to plot the targets, we need to run the pipeline for each case and target
from joblib import Parallel, delayed  # noqa: E402
from joblib.externals.loky import get_reusable_executor  # noqa: E402

# load in all the case info (note this takes awhile in non-parallel form as it has to
# run all the target information for each case)
# this will return a list of tuples with the case id and the target dataset

print("running the pipeline for each case and target")
parallel = Parallel(n_jobs=32, return_as="generator", backend="loky")
case_operators_with_targets_established_generator = parallel(
    delayed(
        lambda co: (
            co.case_metadata.case_id_number,
            evaluate.run_pipeline(co.case_metadata, co.target),
        )
    )(case_operator)
    for case_operator in case_operators
)
case_operators_with_targets_established = list(
    case_operators_with_targets_established_generator
)
# this will throw a bunch of errors below but they're not consequential. this releases
# the memory as it shuts down the workers
get_reusable_executor().shutdown(wait=True)


def get_cbss_and_pph_outputs(ewb_case, forecast_source):
    pph_target = inputs.PPH()
    pph = evaluate.run_pipeline(ewb_case, pph_target)
    cbss = evaluate.run_pipeline(ewb_case, forecast_source)

    return cbss, pph


cira_severe_convection_forecast_GC_GFS = inputs.KerchunkForecast(
    source="gs://extremeweatherbench/GRAP_v100_GFS.parq",
    variables=[derived.CravenBrooksSignificantSevere()],
    variable_mapping=inputs.CIRA_metadata_variable_mapping,
    storage_options={"remote_protocol": "s3", "remote_options": {"anon": True}},
    name="CIRA GC GFS",
    preprocess=defaults._preprocess_bb_severe_cira_forecast_dataset,
)

cira_severe_convection_forecast_FOURV2_GFS = inputs.KerchunkForecast(
    source="gs://extremeweatherbench/FOUR_v200_GFS.parq",
    variables=[derived.CravenBrooksSignificantSevere()],
    variable_mapping=inputs.CIRA_metadata_variable_mapping,
    storage_options={"remote_protocol": "s3", "remote_options": {"anon": True}},
    name="CIRA FOURv2 GFS",
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

my_ids = [
    36,
    37,
    38,
    39,
    40,
    41,
    42,
    43,
    44,
    45,
    46,
    47,
    48,
    49,
    50,
    51,
    52,
    53,
    54,
    55,
    56,
    57,
    58,
    59,
    60,
    61,
    62,
    63,
    64,
    65,
    66,
    67,
    68,
    69,
    269,
    270,
    271,
    272,
    273,
    274,
    275,
    276,
    277,
    278,
    279,
    280,
    281,
    282,
    283,
    284,
    285,
    286,
    287,
    288,
    316,
    317,
    318,
    319,
    320,
    321,
    322,
    323,
    324,
    325,
    326,
    327,
    328,
    329,
    330,
    331,
    332,
    333,
    334,
    335,
    336,
    337,
]


# hres_graphics = dict()
gc_graphics = dict()
pang_graphics = dict()
fourv2_graphics = dict()
hres_graphics = dict()
aifs_graphics = dict()

for my_id in my_ids:
    # compute CBSS and PPH for all the AI models and HRES for the case we chose
    print(my_id)
    my_case = ewb_cases.select_cases("case_id_number", my_id).cases[0]

    # print("Computing CBSS and PPH for HRES")
    # [cbss, pph] = get_cbss_and_pph_outputs(my_case, hres_severe_forecast)
    # hres_graphics[my_id, "cbss"] = cbss
    # hres_graphics[my_id, "pph"] = pph

    # print("Computing CBSS and PPH for FOURV2")
    # [cbss, pph] = get_cbss_and_pph_outputs(
    #     my_case, cira_severe_convection_forecast_FOURV2_GFS
    # )
    # fourv2_graphics[my_id, "cbss"] = cbss
    # fourv2_graphics[my_id, "pph"] = pph

    # print("Computing CBSS and PPH for GC")
    # [cbss, pph] = get_cbss_and_pph_outputs(
    #     my_case, cira_severe_convection_forecast_GC_GFS
    # )
    # gc_graphics[my_id, "cbss"] = cbss
    # gc_graphics[my_id, "pph"] = pph

    # print("Computing CBSS and PPH for PANG")
    # [cbss, pph] = get_cbss_and_pph_outputs(
    #     my_case, cira_severe_convection_forecast_PANG_GFS
    # )
    # pang_graphics[my_id, "cbss"] = cbss
    # pang_graphics[my_id, "pph"] = pph

    print("Computing CBSS and PPH for AIFS")
    [cbss, pph] = get_cbss_and_pph_outputs(
        my_case, aifs_severe_forecast
    )
    aifs_graphics[my_id, "cbss"] = cbss
    aifs_graphics[my_id, "pph"] = pph

print("Saving the graphics objects")
pickle.dump(hres_graphics, open(basepath + "saved_data/hres_graphics.pkl", "wb"))
pickle.dump(gc_graphics, open(basepath + "saved_data/gc_graphics.pkl", "wb"))
pickle.dump(pang_graphics, open(basepath + "saved_data/pang_graphics.pkl", "wb"))
pickle.dump(fourv2_graphics, open(basepath + "saved_data/fourv2_graphics.pkl", "wb"))
