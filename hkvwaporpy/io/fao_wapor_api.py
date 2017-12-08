import requests
import pandas as pd
import datetime

class __fao_wapor_class(object):
    """
    This class object provides functions to the WAPOR service provided through FAO API services
    """
    def __init__(self):
        self._fao_sdi_data_discovery = 'https://api.fao.org/api/sdi/data/discovery/en/workspaces/WAPOR/cubes'
        self._fao_sdi_data_query = 'https://api.fao.org/api/sdi/data/query'
        self._fao_wapor_download = 'http://www.fao.org/wapor-download/WAPOR/coverages/mosaic'
    
    def get_catalogus(self, overview=False):
        """
        Retrieve catalogus of all available datasets on WaPOR

        Parameters
        ----------
        overview : boolean
            type of catalogus:
            overview = False -> full catalogus including additionalInfo and operations
            overview = True  -> compact overview of catalogus

        Returns
        -------
        df : pd.DataFrame
            dataframe containing the catalogus
        """
        # create url
        meta_data_url = '{0}?overview={1}'.format(self._fao_sdi_data_discovery, overview)

        # get request
        resp = requests.get(meta_data_url)

        # parse to dataframe
        meta_data_items = resp.json()['response']['items']
        df = pd.DataFrame.from_dict(meta_data_items, orient='columns')
        return df            

    
    def get_additional_info(self, df, cube_code='L2_AET_D'):
        """
        get additional info from dataset

        Parameters
        ----------
        df : pd.DataFrame
            catalogus dataframe [from get_catalogus()]
        code : dataset of interest
        Returns
        -------
        df_add_info : pd.DataFrame
            dataframe containing detailed information of the dataset
        """
        # select dataset of interest
        df_interest = df.loc[df['code'] == cube_code]
        idx_interest = df_interest.index[0]

        # get additional information datatset
        df_add_info = pd.DataFrame.from_dict(
            df_interest['additionalInfo'].to_dict()[idx_interest], 
            orient='index')
        df_add_info.columns = [cube_code]
        return df_add_info    


    def _query_data_availability(self, ds_row,dimensions_range, period):
        """
        function to retrieve overview of data availability

        Parameters
        ----------
        df_row : pd.DataFrame
            single row of catalogus dataframe
        dimensions_range : list
            list containing start and end date

        Returns
        -------
        resp : json
            json object describing the data availability    
        """    

        query_data_availability = {
            "type":"MDAQuery_Table",
            "params":{  
              "cube":{  
                 "code":ds_row.at[(ds_row.index[0],'code')],
                 "workspaceCode":ds_row.at[(ds_row.index[0],'workspaceCode')],
                 "language":"en"
              },
              "dimensions":[  
                 {  
                    "code":period,
                    "range":dimensions_range
                 }
              ],
              "measures":[  
                 ds_row.at[(ds_row.index[0],'code')][3:]
              ],
              "projection":{  
                 "columns":[  
                    "MEASURES"
                 ],
                 "rows":[  
                    period
                 ]
              },
              "properties":{  
                 "metadata":True,
                 "paged":False
              }            
            }        
        }    
        resp = requests.post(self._fao_sdi_data_query, json=query_data_availability)
        resp = resp.json()
        if 'error' in resp and resp['error'] in ['Bad Request','Internal Server Error']:
            print('Error type: {0}\nMessage is: {1}'.format(resp['error'],resp['message']))
            return None
        return resp    


    def get_data_availability(self, ds_row, dimensions_range='[2014-11-01,2016-01-01]'):
        """
        Function to retrieve overview of data availability

        Parameters
        ----------
        df_row : pd.DataFrame
            single row of catalogus dataframe
        dimensions_range : list
            list containing start and end date

        Returns
        -------
        df_data_avail
        """

        if ds_row.at[(ds_row.index[0],'code')][:2] == 'L2':
            period = 'DEKAD'
        else:
            period = 'YEAR'

        resp = self._query_data_availability(ds_row, dimensions_range, period)

        raster_id_list = []
        start_dekad_list = []
        end_dekad_list = []
        year_list = []

        for item in resp['response']['items']:    
            date_value = item[0]['value']
            raster_id = item[1]['metadata']['rasterId']    
            try:        
                # parse dataframe for annual values            
                year = datetime.datetime(year=int(date_value),month=12,day=31)            
                # append yo list
                raster_id_list.append(raster_id)
                year_list.append(year)

            except Exception as e:
                # parse dataframe for dekad values
                year = int(date_value[0:4])
                month = int(date_value[5:7])
                from_day = int(date_value[13:15])
                to_day = int(date_value[19:21])

                start_dekad = datetime.datetime(year, month, from_day)
                end_dekad = datetime.datetime(year, month, to_day)

                raster_id_list.append(raster_id)
                start_dekad_list.append(start_dekad)
                end_dekad_list.append(end_dekad)        
        if year_list:        
            df = pd.DataFrame(list(zip(year_list, raster_id_list)),
                              columns=['year', 'raster_id'])
            df.loc[:,'year'] = df['year'].dt.strftime('%Y')
            df.set_index('year', inplace=True)
        else:
            df = pd.DataFrame(list(zip(start_dekad_list, end_dekad_list, raster_id_list)),
                              columns=['start_dekad', 'end_dekad', 'raster_id'])            
            df.loc[:,'year'] = df['start_dekad'].dt.strftime('%Y')
            df.loc[:,'start_dekad'] = df['start_dekad'].dt.strftime('%m%d')
            df.loc[:,'end_dekad'] = df['end_dekad'].dt.strftime('%m%d')
            df.set_index('year', inplace=True)

        return df
    

    def _query_locations(self, filter_value, workspace_code):
        """
        function to get locations

        Parameters
        ----------
        filter_value : str
            choose from BASIN or COUNTRY
        workspace_code : str
            defaults to WAPOR

        Returns
        -------
        resp : json
            json object describing the locations
        """        

        query_location_list = {
           "type":"TableQuery_GetList_1",
           "params":{  
              "table":{  
                 "workspaceCode":workspace_code,
                 "code":"LOCATION"
              },
              "properties":{  
                 "paged":False
              },
              "filter":[  
                 {  
                    "columnName":"type",
                    "values":[  
                       filter_value
                    ]
                 },
                 {  
                    "columnName":"l2",
                    "values":[  
                       True
                    ]
                 }
              ],
              "sort":[  
                 {  
                    "columnName":"name"
                 }
              ]
           }        
        }
        resp = requests.post(self._fao_sdi_data_query, json=query_location_list)
        resp = resp.json()
        if 'error' in resp and resp['error'] in ['Bad Request','Internal Server Error']:
            print('Error type: {0}\nMessage is: {1}'.format(resp['error'],resp['message']))
            return None    
        return resp
    

    # get locations of data availability
    def get_locations(self, filter_value=None, workspace_code='WAPOR'):
        """
        Function to get locations of countries or basins of specific workspace

        Parameters
        ----------
        filter_value : str
            choose from 'BASIN' or 'COUNTRY' or None (default None)
        workspace_code : str
            default 'WAPOR'
        Returns
        -------
        df : pd.DataFrame
            dataframe containing name, code, type and bbox of all known locations
        """

        # initate empty lists to fill
        loc_name = []
        loc_code = []
        loc_type = []
        loc_bbox = []    

        # get info of all locations
        if filter_value == None:
            for fil_val in ['BASIN', 'COUNTRY']:
                resp = self._query_locations(filter_value=fil_val, workspace_code=workspace_code)
                for loc in resp['response']:
                    loc_name.append(loc['name'])
                    loc_code.append(loc['code'])
                    loc_type.append(loc['type'])
                    loc_bbox.append(list(map(float, loc['bbox'].split(','))))

        # if filter value is BASIN or COUNTRY
        elif filter_value in ['BASIN', 'COUNTRY']:
            resp = self._query_locations(filter_value, workspace_code)
            for loc in resp['response']:
                loc_name.append(loc['name'])
                loc_code.append(loc['code'])
                loc_type.append(loc['type'])
                loc_bbox.append(list(map(float, loc['bbox'].split(','))))

        # error
        else:
            print('filter_value {} unknown, choose from BASIN, COUNTRY or None'.format(filter_value))
            return

        # parse lists to dataframe
        df = pd.DataFrame(list(zip(loc_name, loc_code, loc_type, loc_bbox)),
                          columns=['name', 'code', 'type', 'bbox'])    

        return df  
    
    
    def get_coverage_url(self, cube_code, year, raster_id, location_code=''):
        """
        function to retrieve a coverage given dataset, date and location
        """
        # build url for L2 products
        if cube_code[:2] == 'L2':
            url_coverage = ('{}/CLIPPED/'.format(self._fao_wapor_download)+
                            '{0}/{1}/{2}/{3}_{4}.tif'.format(                                
                                cube_code,
                                year,
                                raster_id[-4:],
                                raster_id,
                                location_code
                            ))

        # build url for L1 products
        elif cube_code[:2] == 'L1':
            url_coverage = ('{}/'.format(self._fao_wapor_download)+
                            '{0}/{1}.tif'.format(                                
                                cube_code,
                                raster_id
                            ))        

        return url_coverage    