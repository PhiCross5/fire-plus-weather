# fire-plus-weather
## This library requires the netCDF4 python module and an internet connection.
Python library for appending weather information to INPE wildifire spots on the fly.
## instructions
* acquire a CSV file with fire spot data from INPE-queimadas; sort the rows by date-time;
* use `weather_fromCSV()` to get weather data related to each fire spot on-the-fly(check comments in the source itself for the exact syntax);
You can modify `getWeather_point()` to get an arbitrary list or tuple of weather data; variable names used there must also be present at the query string at `url()` and be valid variables within the NOAA GFS NetCDF Subset database (NCSS). Check the url at the bottom of [this NCSS page](https://www.ncei.noaa.gov/thredds/ncss/model-gfs-g4-anl-files/202103/20210316/gfs_4_20210316_0600_000.grb2/dataset.html) for an example of the exact variable names available (may vary from forecast to forecast)
* for any further questions, file an issue or check the comment lines on the code for the module.
