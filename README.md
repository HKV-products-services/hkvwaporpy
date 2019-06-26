# hkvwaporpy
Hkvwapory is a Python package that connects to the FAO WaPOR webservice.
This package provide options to retrieve the url where the raster information is stored on the server given the parameter, level and location.

# installation
install using pypip:

`pip install hkvwaporpy`

# dependencies
hkvwaporpy depends on the following packages:
- requests
- pandas
- datetime
- json

If you have trouble installing these on Windows, you should try downloading these from https://www.lfd.uci.edu/~gohlke/pythonlibs (and use `pip install path/to/package.whl` to install the package).

# usage package
Import the package

    import hkvwaporpy as hkv    
    
Metadata request for available products.

    # request the catalogus
    df = hkv.read_wapor.get_catalogus()
    df.head()
    
    # get additional info of the dataset given a code and catalogus
    df_add = hkv.read_wapor.get_additional_info(df, cube_code='L2_AET_D')
    
    ..
    
    url = hkv.read_wapor.get_coverage_url(ds_code, year, raster_id, location_code)
    print(url)
    http://www.fao.org/wapor-download/WAPOR/coverages/mosaic/CLIPPED/L2_AET_D/2009/0901/L2_AET_0901_7010.tif
    
Coverage request for single product.

    resp = requests.get(url)
    image_data = BytesIO(resp.content)
    
    filename = uuid.uuid4().hex
    mmap_name = "/vsimem/{}".format(filename)
    gdal.FileFromMemBuffer(mmap_name, image_data.read())
    dataset = gdal.Open(mmap_name)
    
    print(gdal.Info(mmap_name))
    Driver: GTiff/GeoTIFF
    Files: /vsimem/473a357685894899a0f15de1f91b86f3
    Size is 5406, 4529
    Coordinate System is:
    GEOGCS["WGS 84",
        DATUM["WGS_1984",
            SPHEROID["WGS 84",6378137,298.257223563,
                AUTHORITY["EPSG","7030"]],
            AUTHORITY["EPSG","6326"]],
        PRIMEM["Greenwich",0],
        UNIT["degree",0.0174532925199433],
        AUTHORITY["EPSG","4326"]]
    Origin = (37.957809690116008,12.388405738995999)
    Pixel Size = (0.000992063000000,-0.000992063000000)
    Metadata:
      AREA_OR_POINT=Area
    Image Structure Metadata:
      COMPRESSION=LZW
      INTERLEAVE=BAND
    Corner Coordinates:
    Upper Left  (  37.9578097,  12.3884057) ( 37d57'28.11"E, 12d23'18.26"N)
    Lower Left  (  37.9578097,   7.8953524) ( 37d57'28.11"E,  7d53'43.27"N)
    Upper Right (  43.3209023,  12.3884057) ( 43d19'15.25"E, 12d23'18.26"N)
    Lower Right (  43.3209023,   7.8953524) ( 43d19'15.25"E,  7d53'43.27"N)
    Center      (  40.6393560,  10.1418791) ( 40d38'21.68"E, 10d 8'30.76"N)
    Band 1 Block=256x256 Type=Byte, ColorInterp=Gray
      NoData Value=255
      Overviews: 2703x2265, 1352x1133, 676x567, 338x284, 169x142    

# Notebook
A Jupyter Notebook is available in the `notebook` folder with a detailed [example](../master/notebook/example usage hkvwaporpy.ipynb "example usage notebook.ipynb") how to retrieve the url and parse and read this raster using GDAL.

# Credits
HKVWAPORPY is written by
- Mattijn van Hoek m.vanhoek@hkv.nl
- Bich N Tran b.tran@un-ihe.org


# Contact
We at HKV provide expert advice and research in the field of water and safety. Using `hkvwapory` we access the WaPOR dataset to code custom-build operational apps and dashboards for river, coasts and deltas providing early-warnings and forecasts for risk and disaster management.

Interested? Head to https://www.hkv.nl/en/ or drop me a line at m.vanhoek [at] hkv.nl
    
