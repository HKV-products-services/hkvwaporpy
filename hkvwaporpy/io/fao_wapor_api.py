import requests
import pandas as pd
import datetime
import json

class __fao_wapor_class(object):
    """
    This class object provides functions to the WAPOR service provided through FAO API services
    """
    def __init__(self):
        # old:
        # self._fao_sdi_data_discovery = 'https://api.fao.org/api/sdi/data/discovery/en/workspaces/WAPOR/cubes'
        # 'http://www.fao.org/wapor-download/WAPOR/coverages/mosaic'        
        
        # new:
        self._fao_sdi_data_discovery = 'https://io.apps.fao.org/gismgr/api/v1/catalog/workspaces/WAPOR/cubes'
        self._fao_sdi_data_query = 'https://io.apps.fao.org/gismgr/api/v1/query'#'https://api.fao.org/api/sdi/data/query'
        self._fao_wapor_download = 'https://io.apps.fao.org/gismgr/api/v1/download/WAPOR'
        self._fao_wapor_identitytoolkit_url = 'https://www.googleapis.com/identitytoolkit/v3/relyingparty'
        self._fao_wapor_identitytoolkit_key = 'AIzaSyAgnCXnRLfniGG6iMqv8PFSpop41YoOrr4'
        self._fao_wapor_token = ''
        
    
    def _query_catalogus(self, overview=False):
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
        #print(meta_data_url)

        # get request
        resp = requests.get(meta_data_url)

        # parse to dataframe
        meta_data_items = resp.json()['response']['items']
        df = pd.DataFrame.from_dict(meta_data_items, orient='columns')
        self._catalogus = df
        return df            
    
    def get_catalogus(self):
        self._catalogus = self._query_catalogus()
        return self._catalogus
    
    def get_info_cube(self, cube_code='L2_AETI_D'):
        """
        get detailed info from a specific data product available within WaPOR.
        
        Parameters
        ----------
        cube_code : str
            code of dataset of interest [codes can be derived from get_catalogus()]

        Returns
        -------
        df_cube_info : pd.DataFrame
            dataframe containing detailed information of the dataset
        """
        
        # firstly retrieve information from catalogus
        df_add_info = self._query_additional_info(cube_code=cube_code)
        
        # secondly retrieve information from cube dimensions
        # df_season, list_season_values, 
        df_dimensions = self._query_dimensions(cube_code=cube_code)
        
        # thirldy retrieve information from cube measures
        df_measures = self._query_measures(cube_code=cube_code)
        
        # fourthly combine the dataframes
        df_add_info.loc['dimensions', cube_code] = pd.np.nan
        df_add_info.loc['measures', cube_code] = pd.np.nan
        
        df_add_info.at['dimensions', cube_code] = df_dimensions       
        df_add_info.at['measures', cube_code] = df_measures
        
        #return df_add_info, df_season, list_season_values, df_dimensions
        return df_add_info

    
    def _query_additional_info(self, cube_code):
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
        df = self._catalogus
        df_interest = df.loc[df['code'] == cube_code]
        idx_interest = df_interest.index[0]

        # get additional information datatset
        df_add_info = pd.DataFrame.from_dict(
            df_interest['additionalInfo'].to_dict()[idx_interest], 
            orient='index')
        df_add_info.columns = [cube_code]
        
        return df_add_info      
    
    def _query_measures(self, cube_code, overview=False):
        """
        get information regarding the units and measurement

        Parameters
        ----------
        cube_code : str
            dataset of interest
        
        Returns
        -------
        df_dimensions : pd.DataFrame
            dataframe containing measures information of the dataset
        """
        # create url
        measures_data_url = '{0}/{1}/measures?overview={2}'.format(self._fao_sdi_data_discovery, cube_code, overview)

        # get request
        resp = requests.get(measures_data_url)

        # parse to dataframe
        if resp.ok == True:
            measures_data_items = resp.json()['response']['items']
        elif resp.ok == False:        
            print('Request not OK, response was:\n{}'.format(resp.content.decode()))
            raise resp.raise_for_status()
            
        df_measures = pd.DataFrame.from_dict(measures_data_items, orient='columns')
        
        # format dataframe and return
        df_measures = df_measures.set_index('code', drop=False).T        
        
        return df_measures

    def _query_dimensions(self, cube_code, overview=False):
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
        
        # get dimensions members for SEASON and STAGE if available
        dimension_code = df_dimensions.iloc[-1]['code']
        #print(dimension_code)

        list_dimensions = df_dimensions.loc[:, 'code'].values
        #print(list_dimensions)
        df_dimensions = df_dimensions.T
        if 'SEASON' in list_dimensions:
            df_members = self._query_dimension_members(cube_code=cube_code, dimension='SEASON')
            
            list_season_values = df_members.loc[:, 'code'].values
            df_season = df_dimensions[df_dimensions['code']=='SEASON'].reset_index(drop=True)
            #return df_season, list_season_values, df_dimensions
            #df_season.loc['season'] = list_season_values            
            #df_dimensions.loc[df_dimensions['code']=='SEASON'] = df_season
        else:
            df_dimensions.loc['season'] = pd.np.nan#list_season_values = 'none'

        if 'STAGE' in list_dimensions:
            df_members = self._query_dimension_members(cube_code=cube_code, dimension='STAGE')

            list_stage_values = df_members.loc[:, 'code'].values
            df_stage= df_dimensions[df_dimensions['code']=='STAGE'].reset_index(drop=True)

            df_stage.loc['stage'] = list_season_values             
            df_dimensions.loc[df_dimensions['code']=='STAGE'] = df_stage
        else:
            df_dimensions.loc['stage'] = pd.np.nan

        # format dataframe and return
        df_dimensions = df_dimensions.T.set_index('code', drop=False).T
        #df_dimensions.rename(columns={0: cube_code}, inplace=True)        
        return df_dimensions
    
    def _query_dimension_members(self, cube_code, dimension, overview=False, paged=False, sort='code'):
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
    

    def _query_data_availability(self, cube_info, dimensions='none', time_range='none', season_values='none', stage_values='none'):
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
        
        cube_code = cube_info.columns[0]
        cube_dims = cube_info.at['dimensions',cube_code]
        cube_meas = cube_info.at['measures',cube_code]
        
        # get number of dimensions
        dimensions = cube_dims.shape[1]
        
        if dimensions == 1:        
            query_data_availability = {
                "type":"MDAQuery_Table",
                "params":{  
                  "cube":{  
                     "code": cube_code,
                     "workspaceCode": cube_dims.iloc[:,0]['workspaceCode'],
                     "language":"en"
                  },
                  "dimensions":[  
                     {  
                        "code": cube_dims.iloc[:,0]['code'],
                        "range": time_range
                     }
                  ],
                  "measures":[  
                     cube_meas.iloc[:,0]['code']
                  ],
                  "projection":{  
                     "columns":[  
                        "MEASURES"
                     ],
                     "rows":[  
                        cube_dims.iloc[:,0]['code']
                     ]
                  },
                  "properties":{  
                     "metadata":True,
                     "paged":False
                  }            
                }        
            }            
        
        elif dimensions == 2:
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
        elif dimensions == 3:
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


    def get_data_availability(self, cube_info, dimensions='none', time_range='[2014-11-01,2016-01-01]', season_values='none', stage_values='none'):
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
            
        cube_code = cube_info.columns[0]
        cube_dims = cube_info.at['dimensions',cube_code]
        cube_meas = cube_info.at['measures',cube_code]
        
        # get number of dimensions
        dimensions = cube_dims.shape[1]
        
        resp = self._query_data_availability(cube_info, dimensions, time_range, season_values, stage_values)

        raster_id_list = []
        bbox_srid_list = []
        bbox_value_list = []
        start_dekad_list = []
        end_dekad_list = []
        year_list = []
        day_list = []
        season_list = []
        stage_list = []
        
        if dimensions == 1:
            period = cube_dims.iloc[:,0]['code']#dimensions[0]
            print('data_avail_period: {}'.format(period))
            for item in resp['response']['items']:    
                date_value = item[0]['value']
                raster_id = item[1]['metadata']['raster']['id']
                bbox_srid = item[1]['metadata']['raster']['bbox'][0]['srid'] 
                bbox_value = item[1]['metadata']['raster']['bbox'][0]['value']
                
                raster_id_list.append(raster_id)
                bbox_srid_list.append(bbox_srid)
                bbox_value_list.append(bbox_value)
                
                if period in ['ANNUAL','YEAR']:
                    # parse dataframe for annual values            
                    year = datetime.datetime(year=int(date_value),month=12,day=31)            
                    # append yo list
                    
                    year_list.append(year)
                    
                elif period == 'DEKAD':
                    # parse dataframe for dekad values
                    year = int(date_value[0:4])
                    month = int(date_value[5:7])
                    from_day = int(date_value[13:15])
                    to_day = int(date_value[19:21])

                    start_dekad = datetime.datetime(year, month, from_day)
                    end_dekad = datetime.datetime(year, month, to_day)

                    
                    start_dekad_list.append(start_dekad)
                    end_dekad_list.append(end_dekad)        
                    
                elif period == 'DAY':                
                    # parse dataframe for dekad values
                    year = int(date_value[0:4])
                    month = int(date_value[5:7])
                    day = int(date_value[8:10])
                    # parse dataframe for annual values            
                    day = datetime.datetime(year=year, month=month, day=day)
                    # append to list
                    
                    day_list.append(day)
                    
        elif dimensions == 2:
            for item in resp['response']['items']:    
                season_value = item[0]['value']
                date_value = item[1]['value']
                raster_id = item[2]['metadata']['raster']['id'] 
                bbox_srid = item[2]['metadata']['raster']['bbox'][0]['srid'] 
                bbox_value = item[2]['metadata']['raster']['bbox'][0]['value']
                
                # parse dataframe for annual values            
                year = datetime.datetime(year=int(date_value),month=12,day=31)            
                # append yo list
                raster_id_list.append(raster_id)
                bbox_srid_list.append(bbox_srid)
                bbox_value_list.append(bbox_value)                
                year_list.append(year)  
                season_list.append(season_value)
                
        elif dimensions == 3:
            for item in resp['response']['items']:    
                season_value = item[0]['value']
                stage_value = item[1]['value']
                date_value = item[2]['value']
                raster_id = item[3]['metadata']['raster']['id'] 
                bbox_srid = item[3]['metadata']['raster']['bbox'][0]['srid'] 
                bbox_value = item[3]['metadata']['raster']['bbox'][0]['value']                

                # parse dataframe for annual values            
                year = datetime.datetime(year=int(date_value),month=12,day=31)            
                # append to list
                raster_id_list.append(raster_id)
                bbox_srid_list.append(bbox_srid)
                bbox_value_list.append(bbox_value)                 
                year_list.append(year)  
                season_list.append(season_value)
                stage_list.append(stage_value)

        if year_list and not season_list and not stage_list:        
            df = pd.DataFrame(list(zip(year_list, raster_id_list, bbox_srid_list, bbox_value_list)),
                              columns=['year', 'raster_id', 'bbox_srid', 'bbox_value'])
            df.loc[:,'year'] = df['year'].dt.strftime('%Y')
            df.set_index('year', inplace=True)
            
        elif day_list:
            df = pd.DataFrame(list(zip(day_list, raster_id_list, bbox_srid_list, bbox_value_list)),
                              columns=['date', 'raster_id', 'bbox_srid', 'bbox_value'])
                              
            df.loc[:,'year'] = df['date'].dt.strftime('%Y')
            df.set_index('year', inplace=True)        
        
        elif start_dekad_list:
            df = pd.DataFrame(list(zip(start_dekad_list, end_dekad_list, raster_id_list, bbox_srid_list, bbox_value_list)),
                              columns=['start_dekad', 'end_dekad', 'raster_id', 'bbox_srid', 'bbox_value'])            
            df.loc[:,'year'] = df['start_dekad'].dt.strftime('%Y')
            df.loc[:,'start_dekad'] = df['start_dekad'].dt.strftime('%m%d')
            df.loc[:,'end_dekad'] = df['end_dekad'].dt.strftime('%m%d')
            df.set_index('year', inplace=True)
        
        elif year_list and season_list and not stage_list:
            df = pd.DataFrame(list(zip(year_list, raster_id_list, season_list, bbox_srid_list, bbox_value_list)),
                              columns=['year', 'raster_id', 'season', 'bbox_srid', 'bbox_value'])
            df.loc[:,'year'] = df['year'].dt.strftime('%Y')
            df.set_index('year', inplace=True)  

        elif year_list and season_list and stage_list:
            df = pd.DataFrame(list(zip(year_list, raster_id_list, season_list, stage_list, bbox_srid_list, bbox_value_list)),
                              columns=['year', 'raster_id', 'season', 'stage', 'bbox_srid', 'bbox_value'])
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
    
    def _query_token(self, email, password):
        """
        function to get user specific Bearer token.
        make sure you are a registered user at WaPOR (https://wapor.apps.fao.org/sign-in)
        
        Parameters
        ----------
        user_email : str
            registered email address
        user_password : str
            valid password
        """
        
        key = self._fao_wapor_identitytoolkit_key
        base_url = self._fao_wapor_identitytoolkit_url
        verify_password = '{}/verifyPassword'.format(base_url)
        
        # post request to the google identitytoolkit
        resp_vp = requests.post(verify_password, data={'key': key, 
                                                       'email': email, 
                                                       'password': password, 
                                                       'returnSecureToken': 'true'
                                                      })
        # parse results
        resp_vp = resp_vp.json()
        token = resp_vp['idToken']
        display_name = resp_vp['displayName']
        expires_in = int(resp_vp['expiresIn']) # seconds
        self._fao_wapor_token = token
        self._fao_wapor_expires_in = expires_in
        
        #identity = {'token': token, 'display_name': display_name, 'expires_in':expires_in}
        
        #return identity
    
    def _query_account_info(self):
        """
        function to get user specific Bearer token.
        make sure you are a registered user at WaPOR (https://wapor.apps.fao.org/sign-in)
        
        Parameters
        ----------
        user_email : str
            registered email address
        user_password : str
            valid password

        Returns
        -------
        log_in_date : datetime
            last login date as datetime object
        """
        
        key = self._fao_wapor_identitytoolkit_key
        base_url = self._fao_wapor_identitytoolkit_url
        get_account_info = '{}/getAccountInfo'.format(base_url)        
        
        token = self._fao_wapor_token
        # post request to the google identitytoolkit
        resp_ai = requests.post(get_account_info, data={'key': key, 
                                                        'idToken': token
                                                       })       
        
        # parse results
        resp_ai = resp_ai.json()
        last_login_at = int(resp_ai['users'][0]['lastLoginAt'])
        self._fao_wapor_last_login_date = datetime.datetime.fromtimestamp(last_login_at/1000)
    
    
    def _quary_valid_token(self, email, password):
        """
        function to check if current token is still valid
        
        Parameters
        ----------
        
        """
        
        if not len(self._fao_wapor_token):
            # get token
            self._query_token(email, password)
            self._query_account_info()
            
        # expiry time is the last login date + the expiry time in seconds from identity toolkit  
        expiry_date = self._fao_wapor_last_login_date + datetime.timedelta(seconds=self._fao_wapor_expires_in)
        
        # if false, token is expired
        if expiry_date > datetime.datetime.now() == False:
            # request new token
            self._query_token(email,password)
            self._query_account_info()            
            
            expiry_date = self._fao_wapor_last_login_date + datetime.timedelta(seconds=self._fao_wapor_expires_in)
            # if expiry date is still false, something else wrong and return error
            if expiry_date > datetime.datetime.now() == False:
                raise ValueError('could not get valid token')
                
        return self._fao_wapor_token
    
        
    def get_coverage_url(self, email, password, raster_id, cube_code, loc_type, loc_code):
        """
        function to retrieve a coverage URL given dataset, date, location, email and password
        make sure you are a registered user at WaPOR (https://wapor.apps.fao.org/sign-in)
        
        Parameters
        ----------
        user_email : str
            registered email address
        user_password : str
            valid password        
        raster_id : str
            id corresponding to a period for which an observation of the product is stored
        cube_code : str
            code from product of interest
        loc_type : str
            choose from 'BASIN' or 'COUNTRY'
        loc_code : str
            code corresponding to location (get from read_wapor.get_locations())

        Returns
        -------
        coverage_object : dict
            dictionary containing the download URL and expiry time in seconds from request time

        """
        # split cube_code
        cube_code_split = cube_code.split('_')
        cube_level = cube_code_split[0]
        cube_product = cube_code_split[1]
        cube_dimension = cube_code_split[2]
        #print(cube_code_split)

        # set location type abbreviation
        if loc_type == 'BASIN':
            loc_type = 'BAS'
        if loc_type == 'COUNTRY':
            loc_type = 'CTY'  
        
        # generic params for raster data
        language = 'en'
        requestType = 'mapset_raster'
            
        # get cube_code and raster_id for L2 products
        if cube_level == 'L2':
            cubeCode = '{1}_{2}_{3}_{4}'.format(
                self._fao_wapor_download, 
                cube_level, 
                cube_product, 
                loc_type, 
                cube_dimension,
                raster_id,
                loc_code
            )
            rasterId = '{5}_{6}'.format(
                self._fao_wapor_download, 
                cube_level, 
                cube_product, 
                loc_type, 
                cube_dimension,
                raster_id,
                loc_code
            )
            
        # get cube_code and raster_id for L1 products
        if cube_level == 'L1':
            cubeCode= '{1}_{2}_{3}'.format(
                self._fao_wapor_download, 
                cube_level, 
                cube_product, 
                cube_dimension,
                raster_id,
                loc_code
            )    
            rasterId='{4}_{5}'.format(
                self._fao_wapor_download, 
                cube_level, 
                cube_product, 
                cube_dimension,
                raster_id,
                loc_code
            )    
        
        # get new token or reuse if still valid
        token = self._quary_valid_token(email, password)
        
        params = {'language':language, 'requestType':requestType, 'cubeCode':cubeCode, 'rasterId':rasterId}
        headers = {'Authorization': "Bearer " + token}
        cov_base_url = self._fao_wapor_download
        r = requests.get(cov_base_url, params=params, headers=headers)
        resp = r.json()['response']
        
        
        expiry_date = datetime.datetime.now() + datetime.timedelta(seconds=int(resp['expiresIn']))
        coverage_object = {'expiry_datetime': expiry_date, 'download_url': resp['downloadUrl']}
        
        return coverage_object