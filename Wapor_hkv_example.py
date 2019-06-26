# -*- coding: utf-8 -*-
"""
Created on Fri Jun  7 14:53:41 2019

@author: ntr002
"""

import hkvwaporpy as hkv
import requests
import datetime

df=hkv.read_wapor.get_catalogus()
df[['caption','code']]

df_locations = hkv.read_wapor.get_locations()
df_locations[['name','type','code','L1','L2','L3']]

ds_code='L1_AETI_D'
cube_info=hkv.read_wapor.get_info_cube(cube_code=ds_code)
print(cube_info)

cube_info.loc['dimensions',ds_code]

cube_info.loc['measures',ds_code]

df_avail = hkv.read_wapor.get_data_availability(cube_info = cube_info,time_range = '[2008-01-01, 2018-12-31]')
df_avail[['raster_id']]

i=0
cube_period = df_avail.iloc[i]
raster_id = cube_period['raster_id']
print(raster_id)

cov_object = hkv.read_wapor.get_coverage_url(
    APItoken='45580076d042abd3c88fba818b87818fe0093102f54ac7417b2cf9a14d318d5855e0f0e473271517', #APIToken generated from WaPOR portal
    raster_id = raster_id,
    cube_code = ds_code)
print(cov_object)

## L2 location
location = df_locations.iloc[25]
loc_type = location['type']
loc_code = location['code']
print(loc_type, loc_code, location['name'])

ds_code='L2_AETI_D'
cube_info=hkv.read_wapor.get_info_cube(cube_code=ds_code)
print(cube_info)

cube_info.loc['dimensions',ds_code]

cube_info.loc['measures',ds_code]

df_avail = hkv.read_wapor.get_data_availability(cube_info = cube_info,time_range = '[2008-01-01, 2018-12-31]')
df_avail[['raster_id']]

i=0
cube_period = df_avail.iloc[i]
raster_id = cube_period['raster_id']
print(raster_id)

cov_object = hkv.read_wapor.get_coverage_url(
    APItoken='45580076d042abd3c88fba818b87818fe0093102f54ac7417b2cf9a14d318d5855e0f0e473271517', #APIToken generated from WaPOR portal
    raster_id = raster_id,
    cube_code = ds_code,
    loc_type = loc_type,
    loc_code = loc_code
    )
print(cov_object)

output_folder=r'E:\WaPOR\L2\AETI\D'
filename=output_folder+'\{0}.tif'.format(raster_id)
#download raster file
resp = requests.get(cov_object['download_url'])
open(filename,'wb').write(resp.content)
print(filename)

import gdal
import numpy as np
from matplotlib import pyplot as plt
dataset = gdal.Open(filename)
band = dataset.GetRasterBand(1)
NDV=band.GetNoDataValue()
# Open as array
array=dataset.ReadAsArray()
array = np.ma.masked_equal(array, NDV)

im = plt.imshow(array)
plt.colorbar(im)
plt.show()



### loop over df_avail to download all raster available in data cube
for i in range(len(df_avail)):
    cube_period = df_avail.iloc[i]
    raster_id = cube_period['raster_id']
    cov_object = hkv.read_wapor.get_coverage_url(
        APItoken='45580076d042abd3c88fba818b87818fe0093102f54ac7417b2cf9a14d318d5855e0f0e473271517', #APIToken generated from WaPOR portal
        raster_id = raster_id,
        cube_code = ds_code,
        loc_type = loc_type,
        loc_code = loc_code)
    print(cov_object['download_url'])
    if cov_object['expiry_datetime'] > datetime.datetime.now():
        # url still valid
        resp = requests.get(cov_object['download_url'])
        open(output_folder+'/{0}.tif'.format(raster_id),'wb').write(resp.content)
   


