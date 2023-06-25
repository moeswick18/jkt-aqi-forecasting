from collections.abc import ValuesView
from typing import Union
import pandas as pd
import os
import numpy as np
import aqi_formulation


class AQIRawData:
    '''
    A class to combine aqi raw data into one file.

    ...

    Attributes
    ----------
    raw_data_path : str
        path to file that contain data

    Methods
    -------
    load_and_process_pm25_raw_data(which_jakarta: list[str] = ["Central", "South"], measurement_stations: list[str] = ["DKI1 (Bunderan HI)", "DKI3 (Jagakarsa)"]) -> ValuesView:
        combine separated pm2,5 data into one DataFrame
    load_and_process_aqi_raw_data(measurement_stations: list[str] = ['DKI1 (Bunderan HI)', 'DKI2 (Kelapa Gading)', 'DKI3 (Jagakarsa)', 'DKI4 (Lubang Buaya)', 'DKI5 (Kebon Jeruk)']) -> pd.DataFrame:
        combine separated aqi data other than pm2,5 into one DataFrame
    merge_pm25_aqi_raw_data(pm25_raw_data: Union[pd.DataFrame, list[pd.DataFrame]], aqi_data: pd.DataFrame, measurement_stations: list[str] = ['DKI1 (Bunderan HI)', 'DKI3 (Jagakarsa)']) -> pd.DataFrame:
        merge pm2,5 and aqi DataFrame into one DataFrame
    process_daily_aqi_raw_data(aqi_data: pd.DataFrame, measurement_stations: list[str] = ['DKI1 (Bunderan HI)', 'DKI2 (Kelapa Gading)', 'DKI3 (Jagakarsa)', 'DKI4 (Lubang Buaya)', 'DKI5 (Kebon Jeruk)']) -> pd.DataFrame:
        finalize combined aqi DataFrame as daily aqi DataFrame
    '''
    def __init__(self, raw_data_path: str) -> None:
        '''
        Constructs all the necessary attributes for the AQIRawData object.

                Parameters
                ----------
                raw_data_path : str
                    path to file that contain data
        '''
        
        self.__raw_data_path = raw_data_path
    

    def load_and_process_pm25_raw_data(self, which_jakarta: list[str] = ["Central", "South"], measurement_stations: list[str] = ["DKI1 (Bunderan HI)", "DKI3 (Jagakarsa)"]) -> ValuesView:
        '''
        Combine separated pm2,5 data into one DataFrame.

                Parameters
                ----------
                which_jakarta: list[str] = ["Central", "South"]
                    list of Jakarta's cities name
                measurement_stations: list[str] = ["DKI1 (Bunderan HI)", "DKI3 (Jagakarsa)"]
                    list of measurement station

                Returns
                ----------
                combined_pm25_raw_data: ValuesView
                    combined and processed pm2,5 data in form of dict_values that need to be separate depends on how much city
        '''

        # empty dictionary datatype to keep pm2,5 yearly data to combine it
        combined_pm25_raw_data = {}

        # # looping based on cities in Jakarta
        for city in which_jakarta:

            # create empty list for each city
            combined_pm25_raw_data[city] = []

            # looping based on data's available year
            for year in range(2015, 2021+1):

                # load one year data
                pm25_raw_data_file = pd.read_csv(f"{self.__raw_data_path}\\Jakarta{city.capitalize()}_PM2.5_{year}_YTD.csv")
                
                # add loaded data into list
                combined_pm25_raw_data[city].append(pm25_raw_data_file)
            
            # combine data inside list into one DataFrame
            combined_pm25_raw_data[city] = pd.concat(combined_pm25_raw_data[city])

        # looping through cities in Jakarta to manipulate DataFrame and insert station
        for i in range(len(which_jakarta)):

            # convert datetime to datetime datatype
            combined_pm25_raw_data[which_jakarta[i]]['Date (LT)'] = pd.to_datetime(combined_pm25_raw_data[which_jakarta[i]]['Date (LT)'], dayfirst=True, format='mixed')

            combined_pm25_raw_data[which_jakarta[i]] = combined_pm25_raw_data[which_jakarta[i]].loc[combined_pm25_raw_data[which_jakarta[i]]['QC Name'] == 'Valid', :]

            # turn data from hourly to daily average
            combined_pm25_raw_data[which_jakarta[i]] = combined_pm25_raw_data[which_jakarta[i]].groupby(pd.Grouper(freq='D', key='Date (LT)')).mean(numeric_only=True)

            # remove pm2,5 with value less than or equal to 0
            combined_pm25_raw_data[which_jakarta[i]] = combined_pm25_raw_data[which_jakarta[i]][(combined_pm25_raw_data[which_jakarta[i]]['Raw Conc.'] > 0)]

            # remove unnecessary column
            combined_pm25_raw_data[which_jakarta[i]].drop(['Year', 'Month', 'Day', 'Hour', 'AQI', 'NowCast Conc.'], axis=1, inplace=True)
        
            # change pm2,5 datatype into integer
            combined_pm25_raw_data[which_jakarta[i]]['Raw Conc.'] = round(combined_pm25_raw_data[which_jakarta[i]]['Raw Conc.'])
            combined_pm25_raw_data[which_jakarta[i]]['Raw Conc.'] = combined_pm25_raw_data[which_jakarta[i]]['Raw Conc.'].astype(float)

            # reset DataFrame index
            combined_pm25_raw_data[which_jakarta[i]].reset_index(inplace=True)

            # rename columns into its corresponding name
            combined_pm25_raw_data[which_jakarta[i]].rename({'Date (LT)': 'tanggal', 'Raw Conc.': 'pm25(ug/m3)'}, axis=1, inplace=True)

            # insert measurement stations into DataFrame
            combined_pm25_raw_data[which_jakarta[i]].insert(1, 'stasiun', measurement_stations[i], True)
        
        # return combined and processed pm2,5 data in dict_values
        return combined_pm25_raw_data.values()
    

    def load_and_process_aqi_raw_data(self, measurement_stations: list[str] = ['DKI1 (Bunderan HI)', 'DKI2 (Kelapa Gading)', 'DKI3 (Jagakarsa)', 'DKI4 (Lubang Buaya)', 'DKI5 (Kebon Jeruk)']) -> pd.DataFrame:
        '''
        Combine separated aqi data other than pm2,5 into one DataFrame.
                
                Parameters
                ----------
                measurement_stations: list[str] = ['DKI1 (Bunderan HI)', 'DKI2 (Kelapa Gading)', 'DKI3 (Jagakarsa)', 'DKI4 (Lubang Buaya)', 'DKI5 (Kebon Jeruk)']
                    list of measurement station
                
                Returns
                ----------
                combined_aqi_raw_data: pd.DataFrame)
                    DataFrame of combined and processed aqi data
        '''

        # empty list datatype to keep aqi monthly data to combine it
        aqi_raw_data = []

        # looping based on data's available year
        for year in range(2010, 2021+1):

            # load all monthly data in corresponding year
            aqi_raw_data_file = sorted(os.listdir(f"{self.__raw_data_path}\\{year}"))

            # loop monthly data
            for i in range(len(aqi_raw_data_file)):

                # loop each monthly data
                temp_df = pd.read_csv(f"{self.__raw_data_path}\\{year}\\{aqi_raw_data_file[i]}")

                # add loaded data into list
                aqi_raw_data.append(temp_df)

        # combine all the data inside list into one DataFrame
        combined_aqi_raw_data = pd.concat(aqi_raw_data)

        # remove unnecessary column
        combined_aqi_raw_data.drop(['max', 'critical', 'categori'], axis=1, inplace=True)

        # move column into necessary place
        move_column = combined_aqi_raw_data.pop('pm25')
        combined_aqi_raw_data.insert(2, 'pm25(ug/m3)', move_column)

        # convert datetime into datetime datatype
        combined_aqi_raw_data['tanggal'] = pd.to_datetime(combined_aqi_raw_data['tanggal'], dayfirst=True, format='mixed')

        # loop through 6 aqi attributes columns
        for column in combined_aqi_raw_data[['pm25(ug/m3)', 'pm10', 'so2', 'co', 'o3', 'no2']]:

            # convert certain column datatype into numeric
            combined_aqi_raw_data[column] = pd.to_numeric(combined_aqi_raw_data[column], errors='coerce')

        # replace 'DKI5 (Kebon Jeruk) Jakarta Barat' value into 'DKI5 (Kebon Jeruk)'
        combined_aqi_raw_data.replace('DKI5 (Kebon Jeruk) Jakarta Barat', 'DKI5 (Kebon Jeruk)', inplace=True)

        # empty list for collections of unavailable date in each station
        unavailable_dates = []
        
        # loop through list of measurement stations
        for measurement_station in measurement_stations:

            # create Series of unavailable date in combined_aqi_raw_data
            unavailable_date = pd.date_range(start='2010-01-01', end='2021-12-31').difference(combined_aqi_raw_data.loc[combined_aqi_raw_data['stasiun'] == measurement_station, 'tanggal'])
            
            # create DataFrame that contain unavailable date of certain measurement station
            unavailable_date_dataframe = pd.DataFrame({'tanggal': unavailable_date, 'stasiun': measurement_station})

            # insert unavailable_date_dataframe into unavailable_dates list
            unavailable_dates.append(unavailable_date_dataframe)

        # turn the list into one DataFrame
        unavailable_dates = pd.concat(unavailable_dates)

        # insert unavailable date on certain station into combined_aqi_raw_data DataFrame
        combined_aqi_raw_data = pd.concat([combined_aqi_raw_data, unavailable_dates])

        # remove unnecessary value
        combined_aqi_raw_data.loc[(combined_aqi_raw_data['stasiun'] != 'DKI1 (Bunderan HI)') & (combined_aqi_raw_data['stasiun'] != 'DKI3 (Jagakarsa)'), 'pm25(ug/m3)'] = np.nan
        
        # sort DataFrame based on 'stasiun' and 'tanggal' columns
        combined_aqi_raw_data.sort_values(by=['stasiun', 'tanggal'], inplace=True)

        # reset DataFrame index
        combined_aqi_raw_data.reset_index(drop=True, inplace=True)
        
        # return the combined data so it can be load later
        return combined_aqi_raw_data
    

    def merge_pm25_aqi_raw_data(self, pm25_raw_data: Union[pd.DataFrame, list[pd.DataFrame]], aqi_raw_data: pd.DataFrame, measurement_stations: list[str] = ['DKI1 (Bunderan HI)', 'DKI3 (Jagakarsa)']) -> pd.DataFrame:
        '''
        Merge pm2,5 and aqi DataFrame into one DataFrame.

                Parameters
                ----------
                pm25_raw_data: pd.DataFrame
                    DataFrame that contain pm2,5 data
                aqi_raw_data: pd.DataFrame
                    DataFrame that contain aqi_raw_data without pm2,5
                measurement_stations: list[str] = ['DKI1 (Bunderan HI)', 'DKI3 (Jagakarsa)']
                    list of measurement station

                Returns
                ----------
                aqi_raw_data: pd.DataFrame
                    Combined and processed aqi data with pm2,5 in DataFrame
        '''

        # Set 'tanggal' column at aqi_raw_data as index for merging process
        aqi_raw_data = aqi_raw_data.set_index('tanggal')

        # check if pm25_raw_data is single DataFrame or multiple DataFrame inside list. Execute if block below if pm25_raw_data is single DataFrame
        if isinstance(pm25_raw_data, pd.DataFrame):
            '''
            If True, execute this block
            '''

            # Set 'tanggal' column at pm25_raw_data as index for merging process
            pm25_raw_data = pm25_raw_data.set_index('tanggal')
            
            # looping through measurement_stations
            for i in range(len(measurement_stations)):

                # merge pm25_raw_data to aqi_raw_data
                aqi_raw_data.update(pm25_raw_data[['pm25(ug/m3)']], overwrite=True, filter_func=lambda pm25_raw_data: aqi_raw_data['stasiun'] == measurement_stations[i])
        
        # if the condition is False, else block below will be executed because pm25_raw_data is multiple DataFrame inside list
        else:
            '''
            If False, execute this block
            '''

            # looping through pm25_raw_data list
            for i in range(len(pm25_raw_data)):

                # Set 'tanggal' column at pm25_raw_data as index for merging process
                pm25_raw_data[i] = pm25_raw_data[i].set_index('tanggal')
                
                # merge pm25_raw_data to aqi_raw_data
                aqi_raw_data.update(pm25_raw_data[i][['pm25(ug/m3)']], overwrite=True, filter_func=lambda pm25_raw_data: aqi_raw_data['stasiun'] == measurement_stations[i])

        # reset aqi_raw_data index
        aqi_raw_data.reset_index(level=0, inplace=True)
        
        # sort aqi_raw_data by 'stasiun' and 'tanggal' column respectively
        aqi_raw_data.sort_values(by=['stasiun', 'tanggal'], inplace=True)

        # create 'pm25' column that contain pm2,5 aqi data and move it beside pm2,5 concentration data
        aqi_raw_data['pm25'] = [aqi_formulation.concentration_to_aqi('pm25', result) for result in aqi_raw_data['pm25(ug/m3)']]
        aqi_raw_data['pm10(ug/m3)'] = [aqi_formulation.aqi_to_concentration('pm10', result) for result in aqi_raw_data['pm10']]
        aqi_raw_data['so2(ug/m3)'] = [aqi_formulation.aqi_to_concentration('so2', result) for result in aqi_raw_data['so2']]
        aqi_raw_data['co(ug/m3)'] = [aqi_formulation.aqi_to_concentration('co', result) for result in aqi_raw_data['co']]
        aqi_raw_data['o3(ug/m3)'] = [aqi_formulation.aqi_to_concentration('o3', result) for result in aqi_raw_data['o3']]
        aqi_raw_data['no2(ug/m3)'] = [aqi_formulation.aqi_to_concentration('no2', result) for result in aqi_raw_data['no2']]

        move_pm25_column = aqi_raw_data.pop('pm25')
        move_pm10_conc_column = aqi_raw_data.pop('pm10(ug/m3)')
        move_so2_conc_column = aqi_raw_data.pop('so2(ug/m3)')
        move_co_conc_column = aqi_raw_data.pop('co(ug/m3)')
        move_o3_conc_column = aqi_raw_data.pop('o3(ug/m3)')
        move_no2_conc_column = aqi_raw_data.pop('no2(ug/m3)')

        aqi_raw_data.insert(3, 'pm25', move_pm25_column)
        aqi_raw_data.insert(4, 'pm10(ug/m3)', move_pm10_conc_column)
        aqi_raw_data.insert(6, 'so2(ug/m3)', move_so2_conc_column)
        aqi_raw_data.insert(8, 'co(ug/m3)', move_co_conc_column)
        aqi_raw_data.insert(10, 'o3(ug/m3)', move_o3_conc_column)
        aqi_raw_data.insert(12, 'no2(ug/m3)', move_no2_conc_column)

        # looping through aqi attributes in aqi_raw_data DataFrame
        for column in aqi_raw_data[['pm25(ug/m3)', 'pm25', 'pm10(ug/m3)', 'pm10', 'so2(ug/m3)', 'so2', 'co(ug/m3)', 'co', 'o3(ug/m3)', 'o3', 'no2(ug/m3)', 'no2']]:

            # change corresponding column datatype
            aqi_raw_data[column] = pd.to_numeric(aqi_raw_data[column], errors='coerce')

        # reset aqi_raw_data index
        aqi_raw_data.reset_index(drop=True, inplace=True)

        # return the combined data so it can be load later
        return aqi_raw_data


    def process_daily_aqi_raw_data(self, aqi_raw_data: pd.DataFrame, measurement_stations: list[str] = ['DKI1 (Bunderan HI)', 'DKI2 (Kelapa Gading)', 'DKI3 (Jagakarsa)', 'DKI4 (Lubang Buaya)', 'DKI5 (Kebon Jeruk)']) -> pd.DataFrame:
        '''
        Finalize combined aqi DataFrame as daily aqi DataFrame.

                Parameters
                ----------
                aqi_raw_data: pd.DataFrame
                    DataFrame that contain aqi_raw_data with pm2,5
               measurement_stations: list[str] = ['DKI1 (Bunderan HI)', 'DKI2 (Kelapa Gading)', 'DKI3 (Jagakarsa)', 'DKI4 (Lubang Buaya)', 'DKI5 (Kebon Jeruk)']
                    list of measurement station

                Returns
                ----------
                raw_daily_aqi_data: pd.DataFrame
                    daily aqi data
        '''

        # empty dictionary datatype to keep aqi data group by its measurement station
        raw_daily_aqi_data = {}

        # looping through measurement stations list
        for measurement_station in measurement_stations:

            # set 'tanggal' column as aqi_raw_data index
            aqi_raw_data.set_index('tanggal', inplace=True)

            # turn data from daily to daily aqi data
            raw_daily_aqi_data[measurement_station] = aqi_raw_data[aqi_raw_data['stasiun'] == measurement_station].resample('D').mean(numeric_only=True)

            # insert 'stasiun' column into raw_daily_aqi_data
            raw_daily_aqi_data[measurement_station].insert(0, 'stasiun', measurement_station)

            # reset aqi_data index
            aqi_raw_data.reset_index(level=0, inplace=True)

        # combine previously separated raw_daily_aqi_data into one DataFrame
        raw_daily_aqi_data = pd.concat(raw_daily_aqi_data.values())

        # aqi category for 'categori' column
        kategori_ispu = {range(1, 51): 'BAIK', range(51, 101): 'SEDANG', range(101, 201): 'TIDAK SEHAT', range(201, 301): 'SANGAT TIDAK SEHAT'}

        # 'max' column with value of highest value on each row
        raw_daily_aqi_data['max'] = raw_daily_aqi_data[['pm25', 'pm10', 'so2', 'co', 'o3', 'no2']].max(axis=1)

        # 'critical' column with value of column name that contain highest numerical value on each row
        raw_daily_aqi_data['critical'] = raw_daily_aqi_data[['pm25', 'pm10', 'so2', 'co', 'o3', 'no2']].idxmax(axis=1).str.upper()

        # fill null value with 0 in columns that store numerical values
        raw_daily_aqi_data[['pm25(ug/m3)', 'pm25', 'pm10(ug/m3)', 'pm10', 'so2(ug/m3)', 'so2', 'co(ug/m3)', 'co', 'o3(ug/m3)', 'o3', 'no2(ug/m3)', 'no2', 'max']] = raw_daily_aqi_data[['pm25(ug/m3)', 'pm25', 'pm10(ug/m3)', 'pm10', 'so2(ug/m3)', 'so2', 'co(ug/m3)', 'co', 'o3(ug/m3)', 'o3', 'no2(ug/m3)', 'no2', 'max']].fillna(value=0)
        
        # change aqi datatype into integer
        raw_daily_aqi_data[['pm25(ug/m3)', 'pm25', 'pm10(ug/m3)', 'pm10', 'so2(ug/m3)', 'so2', 'co(ug/m3)', 'co', 'o3(ug/m3)', 'o3', 'no2(ug/m3)', 'no2', 'max']] = round(raw_daily_aqi_data[['pm25(ug/m3)', 'pm25', 'pm10(ug/m3)', 'pm10', 'so2(ug/m3)', 'so2', 'co(ug/m3)', 'co', 'o3(ug/m3)', 'o3', 'no2(ug/m3)', 'no2', 'max']])
        raw_daily_aqi_data[['pm25(ug/m3)', 'pm25', 'pm10(ug/m3)', 'pm10', 'so2(ug/m3)', 'so2', 'co(ug/m3)', 'co', 'o3(ug/m3)', 'o3', 'no2(ug/m3)', 'no2', 'max']] = raw_daily_aqi_data[['pm25(ug/m3)', 'pm25', 'pm10(ug/m3)', 'pm10', 'so2(ug/m3)', 'so2', 'co(ug/m3)', 'co', 'o3(ug/m3)', 'o3', 'no2(ug/m3)', 'no2', 'max']].astype(int)
        
        # reset raw_daily_aqi_data index
        raw_daily_aqi_data.reset_index(level=0, inplace=True)

        # empty list to store 'categori' column value to combine it later
        categori = []

        # looping through DataFrame rows
        for i in range(raw_daily_aqi_data.shape[0]):

            # looping through 'kategori_ispu' dictionary
            for key in kategori_ispu.keys():

                # checking if value in 'max' column is equal to 0
                if raw_daily_aqi_data.loc[i, 'max'] == 0:

                    # add the below value to the list
                    categori.append('TIDAK ADA DATA')

                    # break the loop
                    break

                # checking if value in 'max' column is equal one of the value inside 'kategori_ispu' dictionary keys
                elif raw_daily_aqi_data.loc[i, 'max'] in key:

                    # add the below value to the list
                    categori.append(kategori_ispu[key])

                    # break the loop
                    break

                # checking if value in 'max' column is greater than or equal to 301
                elif raw_daily_aqi_data.loc[i, 'max'] >= 301:

                    # add the below value to the list
                    categori.append('BERBAHAYA')

                    # break the loop
                    break

        # 'categori' column contain each aqi category in the corresponding day
        raw_daily_aqi_data['categori'] = pd.Series(categori)

        # revert all 0 and null value to ---
        raw_daily_aqi_data.replace([0, np.nan], '---', inplace=True)

        # sort raw_daily_aqi_data by 'stasiun' and 'tanggal' column respectively
        raw_daily_aqi_data.sort_values(by=['stasiun', 'tanggal'], inplace=True)

        # return the finalized aqi data
        return raw_daily_aqi_data


# define and load pm2,5 directory path
process_pm25_raw_data = AQIRawData(".\\Air Quality Data\\pm2,5")

# load. process, and keep pm2,5 data in these two variables
central_combined_pm25_raw_data, south_combined_pm25_raw_data = [data for data in process_pm25_raw_data.load_and_process_pm25_raw_data()]

# define and load aqi directory path
process_aqi_raw_data = AQIRawData(".\\Air Quality Data")

# load. process, and keep aqi data in this variable
aqi_raw_data = process_aqi_raw_data.load_and_process_aqi_raw_data()

# merge pm2,5 and aqi data that doesn't have pm2,5 data
combined_aqi_raw_data = process_aqi_raw_data.merge_pm25_aqi_raw_data([central_combined_pm25_raw_data, south_combined_pm25_raw_data], aqi_raw_data)

# finalized the aqi raw data
daily_aqi_raw_data = process_aqi_raw_data.process_daily_aqi_raw_data(combined_aqi_raw_data)

# save the aqi data as a single .csv file
daily_aqi_raw_data.to_csv(".\\Air Quality Data\\combined_aqi_data.csv", index=False)
