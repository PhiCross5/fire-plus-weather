import netCDF4
from datetime import datetime, date, time, timezone, timedelta
import math

import urllib
import urllib.request
import shutil
import tempfile
import urllib.error as netError

import socket

import time

import sys

#--BE EXTRA-CAREFUL AROUND THIS PIECE OF CODE
#--redid it about 6 times already, i suck at transforming coordinate systems
#Transformation that maps lat-lon coordinates to grid 4 
#   (lat-lon, half-degree stride, starts at north-west corner)
def transform(lat,lon):
    if(lon<0):
        return (round(180-(lat/(0.5))), round((720+(lon/0.5))))
    return (round(180-(lat/(0.5))), round(lon/(0.5)))

class transform_outOfBounds(Exception):
    pass


#alternative transformation that maps to (y,x) indices at a (top,bot,left,right) rectanguloid subset of grid 4.
#--CAUTION: SUBSET RECTANGLES MUST BE IN GRID 4 COORDINATE SYSTEM (longitudes in [0,360], latitudes in [-90,90] )
def transform_subset(lat,lon,subset=(90,-90,0,360)):
    origin_lat=(float(subset[0]))
    origin_lon=(float(subset[2]))
    height=(float(subset[0]) - float(subset[1]))/0.5
    width=(float(subset[3]) - float(subset[2]))/0.5
    y=round((origin_lat-lat)/0.5)
    if(lon<0):
        x=round(((360+lon)-origin_lon)/0.5)
    else:
        x=round((lon-(origin_lon))/0.5)
        
    #sanity check for subset boundaries and transformed coordinates
    #(inverted_latitudes, inverted_longitudes, zero_crossing) = (origin_lat<subset[1], origin_lon>subset[3], origin_lon<0)
    if(origin_lat<subset[1]):
        raise transform_outOfBounds('top edge must be higher than bottom edge! {'+str(origin_lat)+'<'+str(subset[1])+'}')
    if(origin_lon>subset[3]):
        raise transform_outOfBounds('left edge must be lower than right edge {'+str(origin_lon)+'>'+str(subset[3])+'}')
    if(origin_lon<0):
        raise transform_outOfBounds('left edge is negative ('+str(origin_lon)+'<0)'+'. make sure you are defining your subset within grid 4 longitude range [0,360]')
    if(y>height or x>width or y<0 or x<0):
        raise transform_outOfBounds('indexes out of range: (' +str(y)+'), ('+str(x)+')') 
    else:
        return (int(y),int(x))

#parses a datetime string in the format 'YYYY/MM/DD hh:mm:ss' into a datetime object *FLOORED INTO 3-HOUR INCREMENTS*
#   -datetimes by INPE follow a format similar to ISO 8601
def parse_datetime(datetimeString):
  exactDatetime=datetime.strptime(datetimeString,'%Y/%m/%d %H:%M:%S')
  threeHour=math.floor(exactDatetime.hour/3)*3
  roundedTime=datetime(year=exactDatetime.year, month=exactDatetime.month, day=exactDatetime.day, hour=threeHour, minute=0, second=0)
  return roundedTime
    
#timepadding to print out single-digit numbers as double-digit strings
#example: hour in '3:15PM' can be padded to '03' to print standard time '03:15PM'
def timePadding(doubleDigit):
  if (doubleDigit<10):
    return '0'+str(doubleDigit)
  else:
    return str(doubleDigit)

#get url for a given datetime object (use parse_datetime() to get a datetime object from a string)
def url(dateTime, subset=(90,-90,0,360)):
  #datetimes round downwards in 3-hour increments just like the original bash scripts did.
  sixHour=math.floor(dateTime.hour/6)*6
  threeHour=math.floor((dateTime.hour%6)/3)*3
  dt=datetime(year=dateTime.year, month=dateTime.month, day=dateTime.day, hour=sixHour+threeHour, minute=0, second=0)
  directory='https://www.ncei.noaa.gov/thredds/ncss/model-gfs-g4-anl-files-old'
  directory = directory + '/'+str(dt.year)+timePadding(dt.month)
  directory = directory + '/'+str(dt.year)+timePadding(dt.month)+timePadding(dt.day)
  directory = directory + '/'+'gfsanl_4_'+str(dt.year)+timePadding(dt.month)+timePadding(dt.day)
  directory = directory + '_'+timePadding(sixHour)+'00'
  directory = directory + '_0'+timePadding((threeHour))
  directory = directory + '.grb2'+'?var=Soil_temperature_depth_below_surface_layer&var=Volumetric_Soil_Moisture_Content_depth_below_surface_layer&var=Relative_humidity_height_above_ground&var=u-component_of_wind_height_above_ground&var=v-component_of_wind_height_above_ground&var=Temperature_height_above_ground'
  directory = directory + '&north=' + str(subset[0]) +'&west='+str(subset[2])+'&east='+str(subset[3])+'&south='+str(subset[1])
  directory = directory + '&disableProjSubset=on&horizStride=1'+'&time_start='+dt.isoformat()+'&time_end='+dt.isoformat()
  directory = directory + '&timeStride=1'+'&vertCoord=&addLatLon=true'
  return directory 


#get filename of datetime segment
def fileCode(dateTime):
  sixHour=math.floor(dateTime.hour/6)*6
  threeHour=math.floor((dateTime.hour%6)/3)*3
  dt=datetime(year=dateTime.year, month=dateTime.month, day=dateTime.day, hour=sixHour+threeHour, minute=0, second=0)
  name = 'gfsanl_4_'+str(dt.year)+timePadding(dt.month)+timePadding(dt.day)
  name = name + '_'+timePadding(sixHour)+'00'+'_0'+timePadding((threeHour))+'.nc'
  return name

#get (top,bottom,left,right) boundaries of the cdfData dataset object
def getCorners(cdfData):
    return(cdfData.variables['lat'][0],cdfData.variables['lat'][-1],cdfData.variables['lon'][0],cdfData.variables['lon'][-1])




#return a tuple with weather variables for that given file at specified coordinates
#   CAUTION: LATITUDE AND LONGITUDE ARE NOT RAW COORDINATES BUT INDEXES INTO THE GRIDS.
#       USE transform_subset() TO CONVERT COORDINATES TO INDEXES. 
def getWeather_point(cdfData, lat, lon):
  tsoil=cdfData.variables['Soil_temperature_depth_below_surface_layer'][0][0]
  soilw=cdfData.variables['Volumetric_Soil_Moisture_Content_depth_below_surface_layer'][0][0]
  rh=cdfData.variables['Relative_humidity_height_above_ground'][0][0]
  tmp_firstHeight=cdfData.variables['Temperature_height_above_ground'][0][0]
  ugrd=cdfData.variables['u-component_of_wind_height_above_ground'][0][0]
  vgrd=cdfData.variables['v-component_of_wind_height_above_ground'][0][0]  
  return (tsoil[lat][lon], soilw[lat][lon], rh[lat][lon], tmp_firstHeight[lat][lon], ugrd[lat][lon], vgrd[lat][lon])

#fetch url into a local file
def fetch(url,file,tolerance):
    with urllib.request.urlopen(url,timeout=tolerance) as response:
        shutil.copyfileobj(response,file)
    file.seek(0)
        

#attach weather related to (chronologically sorted) source CSV file into a specified target file.
#--subset_bounds is a (top,bot,west,east) in grid 4 system.
def weather_fromCSV(source_path, target_path, subset_bounds=(90,-90,0,360), csv_index=(1,10,11), verbose=True,tolerance=2):
    csv=open(source_path)
    output=open(target_path,'w')
    lastDatetime=None
    fileIsCached=False
    EOF=False
    cache=tempfile.NamedTemporaryFile(delete=False)
    count=0
    try:
        while(True):
            try:
                #extract date-time, latitude and longitude from next CSV line. 
                line=csv.readline()
                if(line==''): #stop getting data on first empty line or end-of-file.
                    break
                tokens=line.split(',')
                (rawDatetime,lat,lon)=(tokens[csv_index[0]],float(tokens[csv_index[1]]),float(tokens[csv_index[2]]))
                datetime=parse_datetime(rawDatetime)
                if(verbose):
                    print('datetime:',datetime)
                #if cache is empty, load the first piece of data
                if(lastDatetime==None):
                        source=url(datetime,subset=subset_bounds)
                        #if(verbose):
                        #    print('url:',source)
                        fetch(source,cache,tolerance)
                        lastDatetime=datetime
                        time.sleep(1)
                delta=datetime-lastDatetime
                #update cache if datetime is off by 3 hours or more.
                if(delta.days>0 or delta.seconds>(60*60*3)):
                    source=url(datetime,subset=subset_bounds)
                    if(verbose):
                        print('crossed three-hour threshold. Fetching...')#41 chars
                        print('url:',source)
                    fetch(source,cache,tolerance)
                    lastDatetime=datetime
                else:
                    if(verbose):
                        print('same three-hour zone, reusing old file...')#41 chars. feel lucky yet?
                #get variables at specified latitude and longitude from the (presumably updated) cache
                cdf=netCDF4.Dataset(cache.name,'r','NETCDF4')
                (y,x)=transform_subset(lat,lon,getCorners(cdf))
                weatherLine=getWeather_point(cdf,y,x)
                #strip newline from original line and concatenate with weather
                finalLine=line[0:-2]
                for weatherVariable in weatherLine:
                    finalLine=finalLine[0:-2]+','+str(weatherVariable)
                if(verbose):
                    print('SUCCESS!')
                    print(finalLine)
                #print out with leading newline character
                output.write(finalLine+'\n')
            
            #in case of problems fetching the file containing data for a point, 
            #just skip it and try the next line
            except urllib.error.HTTPError as netError:
                print('could not fetch data (code:',str(netError.code),', reason:"',netError.reason,'")')
                print('trying again...')
                time.sleep(1)
                continue
            #if any point is not within the subset you picked, it will be skipped.
            except transform_outOfBounds:
                print('coordinates out of bounds. Skipping to next...')
                continue
            except socket.timeout:
                print('Socket timed out. Skipping to next...')
                continue
            except OSError:
                print('error on HTTP fetch. Skipping to next...')
                continue
    #Save partial progress if anything serious happens
    except KeyboardInterrupt as interrupted:
        output.flush()
        print('problems arised, flushing output.')
        raise Exception(interrupted)
    except Exception as bigProblems:
        output.flush()
        raise Exception(bigProblems)



#NOT FOR GENERAL PURPOSE USE!
#same weather from CSV routine copypasted but it takes weather of
#   about 6 hours before the actual matching 3-hour instant
def weather_fromCSV_minus6(source_path, target_path, subset_bounds=(90,-90,0,360), csv_index=(1,10,11), verbose=True,tolerance=2):
    csv=open(source_path)
    output=open(target_path,'w')
    lastDatetime=None
    fileIsCached=False
    EOF=False
    cache=tempfile.NamedTemporaryFile(delete=False)
    count=0
    try:
        while(True):
            try:
                #extract date-time, latitude and longitude from next CSV line. 
                line=csv.readline()
                if(line==''): #stop getting data on first empty line or end-of-file.
                    break
                tokens=line.split(',')
                (rawDatetime,lat,lon)=(tokens[csv_index[0]],float(tokens[csv_index[1]]),float(tokens[csv_index[2]]))
                
                ###ALL THIS REDUNDANT CODE FOR JUST ONE DIFFERENT LINE?
                ###yes. It's the easiest way to cover my use case.
                ###feel free to refactor if you think people will use it.
                #
                datetime=parse_datetime(rawDatetime)-timedelta(hours=6)
                
                
                if(verbose):
                    print('datetime:',datetime)
                #if cache is empty, load the first piece of data
                if(lastDatetime==None):
                        source=url(datetime,subset=subset_bounds)
                        #if(verbose):
                        #    print('url:',source)
                        fetch(source,cache,tolerance)
                        lastDatetime=datetime
                        time.sleep(1)
                delta=datetime-lastDatetime
                #update cache if datetime is off by 3 hours or more.
                if(delta.days>0 or delta.seconds>(60*60*3)):
                    source=url(datetime,subset=subset_bounds)
                    if(verbose):
                        print('crossed three-hour threshold. Fetching...')#41 chars
                        print('url:',source)
                    fetch(source,cache,tolerance)
                    lastDatetime=datetime
                else:
                    if(verbose):
                        print('same three-hour zone, reusing old file...')#41 chars. feel lucky yet?
                #get variables at specified latitude and longitude from the (presumably updated) cache
                cdf=netCDF4.Dataset(cache.name,'r','NETCDF4')
                (y,x)=transform_subset(lat,lon,getCorners(cdf))
                weatherLine=getWeather_point(cdf,y,x)
                #strip newline from original line and concatenate with weather
                finalLine=line[0:-2]
                for weatherVariable in weatherLine:
                    finalLine=finalLine[0:-2]+','+str(weatherVariable)
                if(verbose):
                    print('SUCCESS!')
                    print(finalLine)
                #print out with leading newline character
                output.write(finalLine+'\n')
            
            #in case of problems fetching the file containing data for a point, 
            #just skip it and try the next line
            except urllib.error.HTTPError as netError:
                print('could not fetch data (code:',str(netError.code),', reason:"',netError.reason,'")')
                print('trying again...')
                time.sleep(1)
                continue
            #if any point is not within the subset you picked, it will be skipped.
            except transform_outOfBounds:
                print('coordinates out of bounds. Skipping to next...')
                continue
            except socket.timeout:
                print('Socket timed out. Skipping to next...')
                continue
            except OSError:
                print('error on HTTP fetch. Skipping to next...')
                continue
    #Save partial progress if anything serious happens
    except KeyboardInterrupt as interrupted:
        output.flush()
        print('problems arised, flushing output.')
        raise Exception(interrupted)
    except Exception as bigProblems:
        output.flush()
        raise Exception(bigProblems)



  
  
#for debug: peek at the coordinates generated by the transform fuction
#--comparing the generated values with the coordinates you entered this should match within +-0.5°... 
#----...except for the longitude, the value of which should match within +-0.5° of (360+(original longitude)) if the 
#------corresponding input value is negative.
def checkCoords(dataset,lat,lon):
    (x,y)=transform_subset(lat,lon,getCorners(dataset))
    return (dataset.variables['lat'][x].data,dataset.variables['lon'][y].data)


