# setup all the imports
import seaborn as sns
from extremeweatherbench import cases, defaults, derived, evaluate, inputs

sns.set_theme(style="whitegrid")
import pickle
from pathlib import Path

# make the basepath - change this to your local path
basepath = Path.home() / "extreme-weather-bench-paper" / ""
basepath = str(basepath) + "/"


# load in all of the events in the yaml file
print("loading in the events yaml file")
ewb_cases = cases.load_ewb_events_yaml_into_case_collection()
# build out all of the expected data to evalate the case
# this will not be a 1-1 mapping with ewb_cases because there are multiple data sources
# to evaluate for some cases
# for example, a heat/cold case will have both a case operator for ERA-5 data and GHCN
case_operators = cases.build_case_operators(
    ewb_cases, defaults.get_brightband_evaluation_objects()
)

my_ids = [112, 113, 114, 116, 117, 119, 120, 121, 122, 123, 124, 125, 127, 128]

# Define forecast (HRES)
hres_forecast = inputs.ZarrForecast(
    source="gs://weatherbench2/datasets/hres/2016-2022-0012-1440x721.zarr",
    name="ECMWF HRES",
    variables=[derived.AtmosphericRiverVariables()],
    variable_mapping=inputs.HRES_metadata_variable_mapping,
)

cira_AR_FOURv2_GFSforecast = inputs.KerchunkForecast(
    source="gs://extremeweatherbench/FOUR_v200_GFS.parq",
    variables=[derived.AtmosphericRiverVariables()],
    variable_mapping=inputs.CIRA_metadata_variable_mapping,
    storage_options={"remote_protocol": "s3", "remote_options": {"anon": True}},
    preprocess=defaults._preprocess_bb_ar_cira_forecast_dataset,
    name="CIRA FOURv2 GFS",
)

cira_AR_GC_GFSforecast = inputs.KerchunkForecast(
    source="gs://extremeweatherbench/GRAP_v100_GFS.parq",
    variables=[derived.AtmosphericRiverVariables()],
    variable_mapping=inputs.CIRA_metadata_variable_mapping,
    storage_options={"remote_protocol": "s3", "remote_options": {"anon": True}},
    preprocess=defaults._preprocess_bb_ar_cira_forecast_dataset,
    name="CIRA GC GFS",
)

cira_AR_PANG_GFSforecast = inputs.KerchunkForecast(
    source="gs://extremeweatherbench/PANG_v100_GFS.parq",
    variables=[derived.AtmosphericRiverVariables()],
    variable_mapping=inputs.CIRA_metadata_variable_mapping,
    storage_options={"remote_protocol": "s3", "remote_options": {"anon": True}},
    preprocess=defaults._preprocess_bb_ar_cira_forecast_dataset,
    name="CIRA PANG GFS",
)

gc_graphics = dict()
pang_graphics = dict()
fourv2_graphics = dict()
hres_graphics = dict()


def get_ivt(ewb_case, forecast_source):
    ivt = evaluate.run_pipeline(ewb_case, forecast_source)

    return ivt


for my_id in my_ids:
    # compute IVT for all the AI models and HRES for the case we chose
    print(my_id)
    my_case = ewb_cases.select_cases("case_id_number", my_id).cases[0]

    # print("Computing IVT for HRES")
    # ivt = get_ivt(my_case, hres_forecast)
    # hres_graphics[my_id] = ivt

    # print("Computing IVT for FOURv2")
    # ivt = get_ivt(my_case, cira_AR_FOURv2_GFSforecast)
    # fourv2_graphics[my_id] = ivt

    # print("Computing IVT for GC")
    # ivt = get_ivt(my_case, cira_AR_GC_GFSforecast)
    # gc_graphics[my_id] = ivt

    print("Computing IVT for PANG")
    ivt = get_ivt(my_case, cira_AR_PANG_GFSforecast)
    pang_graphics[my_id] = ivt

print("Saving the graphics objects")
# pickle.dump(hres_graphics, open(basepath + "saved_data/hres_ar_graphics.pkl", "wb"))
# pickle.dump(fourv2_graphics, open(basepath + "saved_data/fourv2_ar_graphics.pkl", "wb"))
# pickle.dump(gc_graphics, open(basepath + "saved_data/gc_ar_graphics.pkl", "wb"))
pickle.dump(pang_graphics, open(basepath + "saved_data/pang_ar_graphics.pkl", "wb"))
