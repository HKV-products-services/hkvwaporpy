# hkvwaporpy
[![PyPI version](https://img.shields.io/pypi/v/hkvwaporpy.svg)](https://pypi.org/project/hkvwaporpy)
[![License](https://img.shields.io/badge/License-BSD%203--Clause-blue.svg)](https://opensource.org/licenses/BSD-3-Clause)

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
     
A Jupyter Notebook is available in the `notebook` folder with a detailed [example](../master/notebook/example usage hkvwaporpy.ipynb "example usage notebook.ipynb") how to retrieve the url and parse and read this raster using GDAL.

# Credits
HKVWAPORPY is written by
- Mattijn van Hoek m.vanhoek@hkv.nl
With contributions from:
- Bich N Tran b.tran@un-ihe.org


# Contact
We at HKV provide expert advice and research in the field of water and safety. Using `hkvwapory` we access the WaPOR dataset to code custom-build operational apps and dashboards for river, coasts and deltas providing early-warnings and forecasts for risk and disaster management.

Interested? Head to https://www.hkv.nl/en/ or drop me a line at m.vanhoek [at] hkv.nl
    
