import requests
import pandas as pd
import datetime
import json

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

    def get_dimensions(self, cube_code='L2_AET_D', overview=False):
        """
        get (time) dimension info from dataset

        Parameters
        ----------
        cube_code : str
            dataset of interest
        
        Returns
        -------
        df_dimensions : pd.DataFrame
            dataframe containing time dimension information of the dataset
        """
        # create url
        dimensions_data_url = '{0}/{1}/dimensions?overview={2}'.format(self._fao_sdi_data_discovery, cube_code, overview)

        # get request
        resp = requests.get(dimensions_data_url)

        # parse to dataframe
        if resp.ok == True:
            dimensions_data_items = resp.json()['response']['items']
        elif resp.ok == False:        
            print('Request not OK, response was:\n{}'.format(resp.content.decode()))
            raise resp.raise_for_status()
            
        df_dimensions = pd.DataFrame.from_dict(dimensions_data_items, orient='columns')
        return df_dimensions

    def get_dimension_members(self, cube_code, dimension, overview=False, paged=False, sort='code'):
        """
        get dimension members from dataset

        Parameters
        ----------
        cube_code : str
            dataset of interest
        dimension : str
            dimension from dataset       
        
        Returns
        -------
        df_dimension_members : pd.DataFrame
            dataframe containing dimension members
        """
        # create url
        members_data_url = '{0}/{1}/dimensions/{2}/members?overview={3}&paged={4}&sort={5}'.format(
            self._fao_sdi_data_discovery, cube_code, dimension, overview, paged, sort)

        # get request
        resp = requests.get(members_data_url)

        # # parse to dataframe
        members_data_items = resp.json()['response']
        df_members = pd.DataFrame.from_dict(members_data_items, orient='columns')
        return df_members    
    
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


    def _query_data_availability(self, ds_row, dimensions, time_range, season_values='none', stage_values='none'):
        """
        function to retrieve overview of data availability

        Parameters
        ----------
        df_row : pd.DataFrame
            single row of catalogus dataframe
        dimensions : list
            list with dimensions (normally 1, except for SEASON, than 2)
        time_range : list
            list containing start and end date
        season_values : list
            list with values if SEASON is included in dimension list
            if 1 dimension, this is ignored 
        

        Returns
        -------
        resp : json
            json object describing the data availability    
        """
        
        if len(dimensions) == 1:        
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
                        "code":dimensions[0],
                        "range":time_range
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
                        dimensions[0]
                     ]
                  },
                  "properties":{  
                     "metadata":True,
                     "paged":False
                  }            
                }        
            }            
        
        if len(dimensions) == 2:
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
                        "code":dimensions[0],
                        "values":season_values.tolist()
                     },                  
                     {  
                        "code":dimensions[1],
                        "range":time_range
                     }
                  ],
                  "measures":[  
                     ds_row.at[(ds_row.index[0],'code')][3:]
                  ],
                  "projection":{  
                     "columns":[  
                        "MEASURES"
                     ],
                     "rows": dimensions.tolist()
                  },
                  "properties":{  
                     "metadata":True,
                     "paged":False
                  }            
                }        
            }   
        if len(dimensions) == 3:
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
                        "code":dimensions[0],
                        "values":season_values.tolist()
                     },  
                     {  
                        "code":dimensions[1],
                        "values":stage_values.tolist()
                     },                       
                     {  
                        "code":dimensions[2],
                        "range":time_range
                     }
                  ],
                  "measures":[  
                     ds_row.at[(ds_row.index[0],'code')][3:]
                  ],
                  "projection":{  
                     "columns":[  
                        "MEASURES"
                     ],
                     "rows": dimensions.tolist()
                  },
                  "properties":{  
                     "metadata":True,
                     "paged":False
                  }            
                }        
            }   
        # return query_data_availability        
        resp = requests.post(self._fao_sdi_data_query, json=query_data_availability)
        resp = resp.json()
        if 'error' in resp and resp['error'] in ['Bad Request','Internal Server Error']:
            print('Error type: {0}\nMessage is: {1}'.format(resp['error'],resp['message']))
            return None
        return resp    


    def get_data_availability(self, ds_row, dimensions, time_range='[2014-11-01,2016-01-01]', season_values='none', stage_values='none'):
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

        # if ds_row.at[(ds_row.index[0],'code')][:2] == 'L2':
            # period = 'DEKAD'
        # else:
            # period = 'YEAR'
        resp = self._query_data_availability(ds_row, dimensions, time_range, season_values, stage_values)

        raster_id_list = []
        start_dekad_list = []
        end_dekad_list = []
        year_list = []
        day_list = []
        season_list = []
        stage_list = []
        
        if len(dimensions) == 1:
            period = dimensions[0]
            print('data_avaib_period: {}'.format(period))
            for item in resp['response']['items']:    
                date_value = item[0]['value']
                raster_id = item[1]['metadata']['rasterId']
                
                if period in ['ANNUAL','YEAR']:
                    # parse dataframe for annual values            
                    year = datetime.datetime(year=int(date_value),month=12,day=31)            
                    # append yo list
                    raster_id_list.append(raster_id)
                    year_list.append(year)
                    
                elif period == 'DEKAD':
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
                    
                elif period == 'DAY':                
                    # parse dataframe for dekad values
                    year = int(date_value[0:4])
                    month = int(date_value[5:7])
                    day = int(date_value[8:10])
                    # parse dataframe for annual values            
                    day = datetime.datetime(year=year, month=month, day=day)
                    # append yo list
                    raster_id_list.append(raster_id)
                    day_list.append(day)
                    
        if len(dimensions) == 2:
            for item in resp['response']['items']:    
                season_value = item[0]['value']
                date_value = item[1]['value']
                raster_id = item[2]['metadata']['rasterId'] 

                # parse dataframe for annual values            
                year = datetime.datetime(year=int(date_value),month=12,day=31)            
                # append yo list
                raster_id_list.append(raster_id)
                year_list.append(year)  
                season_list.append(season_value)
                
        if len(dimensions) == 3:
            for item in resp['response']['items']:    
                season_value = item[0]['value']
                stage_value = item[1]['value']
                date_value = item[2]['value']
                raster_id = item[3]['metadata']['rasterId'] 

                # parse dataframe for annual values            
                year = datetime.datetime(year=int(date_value),month=12,day=31)            
                # append yo list
                raster_id_list.append(raster_id)
                year_list.append(year)  
                season_list.append(season_value)
                stage_list.append(stage_value)

        if year_list and not season_list and not stage_list:        
            df = pd.DataFrame(list(zip(year_list, raster_id_list)),
                              columns=['year', 'raster_id'])
            df.loc[:,'year'] = df['year'].dt.strftime('%Y')
            df.set_index('year', inplace=True)
            
        elif day_list:
            df = pd.DataFrame(list(zip(day_list, raster_id_list)),
                              columns=['date', 'raster_id'])
                              
            df.loc[:,'year'] = df['date'].dt.strftime('%Y')
            df.set_index('year', inplace=True)        
        
        elif start_dekad_list:
            df = pd.DataFrame(list(zip(start_dekad_list, end_dekad_list, raster_id_list)),
                              columns=['start_dekad', 'end_dekad', 'raster_id'])            
            df.loc[:,'year'] = df['start_dekad'].dt.strftime('%Y')
            df.loc[:,'start_dekad'] = df['start_dekad'].dt.strftime('%m%d')
            df.loc[:,'end_dekad'] = df['end_dekad'].dt.strftime('%m%d')
            df.set_index('year', inplace=True)
        
        elif year_list and season_list and not stage_list:
            df = pd.DataFrame(list(zip(year_list, raster_id_list, season_list)),
                              columns=['year', 'raster_id', 'season'])
            df.loc[:,'year'] = df['year'].dt.strftime('%Y')
            df.set_index('year', inplace=True)  

        elif year_list and season_list and stage_list:
            df = pd.DataFrame(list(zip(year_list, raster_id_list, season_list, stage_list)),
                              columns=['year', 'raster_id', 'season', 'stage'])
            df.loc[:,'year'] = df['year'].dt.strftime('%Y')
            df.set_index('year', inplace=True)              

        return df.sort_index()
    

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
    
    
    def get_coverage_url(self, cube_code, year, raster_id, location_code='', dimension_code=''):
        """
        function to retrieve a coverage given dataset, date and location
        """
        
        # build url for L2 products
        if cube_code[:2] == 'L2':
            if any(x in raster_id for x in ['_e', '_m', '_s']):
                slice_number = -6
            else:
                slice_number = -4
            url_coverage = ('{}/CLIPPED/'.format(self._fao_wapor_download)+
                            '{0}/{1}/{2}/{3}_{4}.tif'.format(                                
                                cube_code,
                                year,
                                raster_id[slice_number:],
                                raster_id,
                                location_code
                            ))

        # build url for L1 products
        elif cube_code[:2] == 'L1':
            if dimension_code in ['DEKAD','DAY']:
                url_coverage = ('{}/'.format(self._fao_wapor_download)+
                                '{0}/{1}/{2}.tif'.format(                                
                                    cube_code,
                                    year,
                                    raster_id
                                )) 
            
            else:
                url_coverage = ('{}/'.format(self._fao_wapor_download)+
                                '{0}/{1}.tif'.format(                                
                                    cube_code,
                                    raster_id
                                ))        

        return url_coverage    