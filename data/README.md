`details-1950.csv` contains the 223 1950 storm-event records used by this
project. It was created from the official NOAA/NCEI Storm Events details file:

`StormEvents_details-ftp_v1.0_d1950_c20260323.csv.gz`

Source: https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/

The project file retains only the 14 assignment fields, in the required order:
event_id, state, year, month_name, event_type, cz_type, cz_name,
injuries_direct, injuries_indirect, deaths_direct, deaths_indirect,
damage_property, damage_crops, tor_f_scale.

Synthetic data used by automated tests is created only in temporary directories.
