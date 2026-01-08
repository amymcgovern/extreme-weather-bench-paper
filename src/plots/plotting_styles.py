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
gc_style = {"color": accessible_colors[2]}
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
fourv2_ifs_cira_settings = {
    "forecast_source": "CIRA FOURv2 IFS",
    "label_str": "ForecastNet V2",
}
fourv2_gfs_cira_settings = {
    "forecast_source": "CIRA FOURv2 GFS",
    "label_str": "ForecastNet V2",
}
gc_ifs_cira_settings = {"forecast_source": "CIRA GC IFS", "label_str": "GraphCast"}
gc_gfs_cira_settings = {"forecast_source": "CIRA GC GFS", "label_str": "GraphCast"}
pangu_ifs_cira_settings = {
    "forecast_source": "CIRA PANG IFS",
    "label_str": "Pangu Weather",
}
pangu_gfs_cira_settings = {
    "forecast_source": "CIRA PANG GFS",
    "label_str": "Pangu Weather",
}
pangu_bb_settings = {"forecast_source": "BB Pangu", "label_str": "Pangu Weather"}
gc_bb_settings = {"forecast_source": "BB Graphcast", "label_str": "GraphCast"}
aifs_bb_settings = {"forecast_source": "BB AIFS", "label_str": "AIFS"}
hres_ifs_settings = {"forecast_source": "ECMWF HRES", "label_str": "HRES"} 

fourv2_ifs_settings = fourv2_ifs_cira_settings | fourv2_style | ifs_group_style
gc_ifs_settings = gc_ifs_cira_settings | gc_style | ifs_group_style
pangu_ifs_settings = pangu_ifs_cira_settings | pangu_style | ifs_group_style
hres_settings = hres_ifs_settings | hres_style | hres_group_style
aifs_ifs_settings = aifs_bb_settings | aifs_style | ifs_group_style
pangu_bb_ifs_settings = pangu_bb_settings | pangu_style | ifs_group_style
gc_bb_ifs_settings = gc_bb_settings | gc_style | ifs_group_style


fourv2_gfs_settings = fourv2_gfs_cira_settings | fourv2_style | gfs_group_style
gc_gfs_settings = gc_gfs_cira_settings | gc_style | gfs_group_style
pangu_gfs_settings = pangu_gfs_cira_settings | pangu_style | gfs_group_style


severe_tp_settings = {"linestyle": "-", "marker": "o", "group": "True Positives"}
severe_fn_settings = {"linestyle": "--", "marker": "x", "group": "False Negatives"}
