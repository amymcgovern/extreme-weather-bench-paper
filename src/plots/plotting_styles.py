# make a global color palatte so things are consistent across plots
import seaborn as sns

sns_palette = sns.color_palette("tab10")
sns.set_style("whitegrid")

accessible_colors = [
    "#3394D6",  # Blue
    "#E09000",  # Orange
    "#A15A7E",  # Reddish purple
    "#CC4A4A",  # Vermillion
    "#A0A0A0",  # Grey
    "#B2B24D",  # Olive
    "#33B890",  # Bluish green
    "#78C6F1",  # Sky blue
    "#F0E442",  # Yellow
]

# defaults for plotting
fourv2_style = {"color": accessible_colors[0]}
graphcast_style = {"color": accessible_colors[2]}
pangu_style = {"color": accessible_colors[3]}
hres_style = {"color": "black"}
aifs_style = {"color": accessible_colors[5]}

# the group styles and settings so that we can just
# easily grab them for the plots and they are globally consistent

ghcn_group_style = {"linestyle": "-", "marker": "o", "group": "GHCN"}
era5_group_style = {"linestyle": "--", "marker": "s", "group": "ERA5"}

ifs_group_style = {"linestyle": "-", "marker": "o", "group": "IFS"}
gfs_group_style = {"linestyle": ":", "marker": "d", "group": "GFS"}

global_group_style = {"linestyle": "--", "marker": "*", "group": "Global"}

hres_group_style = {"linestyle": "-", "marker": ".", "group": "HRES"}

# settings for the different models
cira_fourv2_ifs_settings = {
    "forecast_source": "CIRA FOURv2 IFS",
    "label_str": "ForecastNet V2",
}
cira_fourv2_gfs_settings = {
    "forecast_source": "CIRA FOURv2 GFS",
    "label_str": "ForecastNet V2",
}
cira_graphcast_ifs_settings = {"forecast_source": "CIRA GC IFS", "label_str": "GraphCast"}
cira_graphcast_gfs_settings = {"forecast_source": "CIRA GC GFS", "label_str": "GraphCast"}
cira_pangu_ifs_settings = {
    "forecast_source": "CIRA PANG IFS",
    "label_str": "Pangu Weather",
}
cira_pangu_gfs_settings = {
    "forecast_source": "CIRA PANG GFS",
    "label_str": "Pangu Weather",
}
bb_pangu_settings = {"forecast_source": "BB panguweather", "label_str": "Pangu Weather"}
bb_graphcast_settings = {"forecast_source": "BB graphcast", "label_str": "GraphCast"}
bb_aifs_settings = {"forecast_source": "BB aifs-single", "label_str": "AIFS"}
hres_settings = {"forecast_source": "ECMWF HRES", "label_str": "HRES"} 

# combine the settings for the different models
cira_fourv2_ifs_settings = cira_fourv2_ifs_settings | fourv2_style | ifs_group_style
cira_fourv2_gfs_settings = cira_fourv2_gfs_settings | fourv2_style | gfs_group_style
cira_graphcast_ifs_settings = cira_graphcast_ifs_settings | graphcast_style | ifs_group_style
cira_graphcast_gfs_settings = cira_graphcast_gfs_settings | graphcast_style | gfs_group_style
cira_pangu_ifs_settings = cira_pangu_ifs_settings | pangu_style | ifs_group_style
cira_pangu_gfs_settings = cira_pangu_gfs_settings | pangu_style | gfs_group_style
bb_pangu_settings = bb_pangu_settings | pangu_style | global_group_style
bb_graphcast_settings = bb_graphcast_settings | graphcast_style | global_group_style
bb_aifs_settings = bb_aifs_settings | aifs_style | global_group_style
hres_ifs_settings = hres_settings | hres_style | ifs_group_style

severe_tp_settings = {"linestyle": "-", "marker": "o", "group": "True Positives"}
severe_fn_settings = {"linestyle": "--", "marker": "x", "group": "False Negatives"}
