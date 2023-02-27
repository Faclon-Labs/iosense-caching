# import libraries
import json
import os
import re
import sys
import time

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
import urllib3

pd.options.mode.chained_assignment = None
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from datetime import timedelta, datetime


class DataAccess:

    def __init__(self, apikey, url, cpath):
        self.apikey = apikey
        self.url = url
        self.cpath = cpath

    def check_in_cache(self, device_id, sensors, start_time, end_time):
        '''

        Check if data exists in cache
        ____________________________
        :param device_id: input given by user e.g. TPEM_B3
        :param sensors: sensor IDs
        :param start_time: date from where you want to fetch data
        :param end_time: data until you want data
        :return: message, incache_list,nocache_list,senor_list(if sensor=None)

        if required data exists in cache send msg -> yes
        if required data does not exists in cache send msg -> no

        '''
        msg = ''
        date_list = []
        str_dates = []
        incache = []
        nocache = []
        before_date_in_cache = []
        after_date_in_cache = []

        def sort_dates(dates):
            def date_key(date_string):
                return datetime.strptime(date_string, '%Y-%m-%d')
            return sorted(dates, key=date_key)

        match_start_str = re.search(r'\d{4}-\d{2}-\d{2}', start_time)
        match_end_str = re.search(r'\d{4}-\d{2}-\d{2}', end_time)

        startT = (datetime.strptime(match_start_str.group(), '%Y-%m-%d').date())
        endT = (datetime.strptime(match_end_str.group(), '%Y-%m-%d').date())
        curr_date = startT
        while curr_date <= endT:
            date_list.append(curr_date)
            curr_date += timedelta(days=1)
        for i_s in date_list:
            i_s = str(i_s)
            str_dates.append(i_s)
        if sensors == None:
            raw_metadata = DataAccess.get_device_metadata(self, device_id)
            sensor_spec = 'sensors'
            sensor_param_df = pd.DataFrame(raw_metadata[sensor_spec])
            sensor_list = sensor_param_df['sensorId'].tolist()
        else:
            sensor_list = sensors

        for j in sensor_list:
            for i in str_dates:
                try:
                    path = str(self.cpath) + device_id + "/sensor=" + j + "/Date=" + i + "/"
                    isExist = os.path.isdir(path)
                    if isExist:
                        incache.append(i)
                    else:
                        nocache.append(i)
                except OSError as e:
                    nocache.append(i)
        incache = [*set(incache)]
        nocache = [*set(nocache)]

        sorted_dates = sort_dates(nocache)
        incache = sort_dates(incache)

        for val in sorted_dates:
            for val1 in incache:
                if val < val1:
                    before_date_in_cache.append(val)
                if val > val1:
                    after_date_in_cache.append(val)

        before_date_in_cache = [*set(before_date_in_cache)]
        before_date_in_cache = sort_dates(before_date_in_cache)
        after_date_in_cache = [*set(after_date_in_cache)]
        after_date_in_cache = sort_dates(after_date_in_cache)
        if len(incache) != 0:
            msg = 'yes'
        else:
            msg = 'no'
        return msg, incache, nocache, sensor_list

    def get_data_from_cache(self, incache, sensor_list, device_id):
        '''
        :param incache: dates in cache list
        :param sensor_list: sensor list
        :param device_id: input given by user
        :return: DaTaframe

        Fetch data from cache
        ____________________________

        Check if given date and sensor exists in path.
        If path exists read the parquet file

        '''
        df_new = pd.DataFrame(columns=["time", "del", "value", "sensor"])
        for i in range(len(incache)):
            sys.stdout.write('\r')
            sys.stdout.write("Please Wait ...")
            sys.stdout.flush()

        for k in sensor_list:
            for l in incache:
                path = str(self.cpath) + device_id + "/sensor=" + k + "/Date=" + l + "/"
                dir = os.listdir(path)
                if len(dir) == 2:
                    path = str(self.cpath) + device_id + "/sensor=" + k + "/Date=" + l + "/"+dir[0]
                df = pd.read_parquet(path)
                df['sensor'] = k
                df_new = pd.concat([df_new, df])
        return df_new

    def store_data_in_cache(self, device_id, df, sensors):
        '''

        :param device_id: 'ABCD'
        :param df:
        :param sensors: 'AB29'
        :return: success

        Store Data in Cache
        ____________________________

        Store Dataframe in local storage using parquet file format
        Format of directory is as follows
        ---TPEM_B3
         ---sensor=D29
            ---Date=2022-09-02
                --00909000.parquet
         ---sensor=D30
            ---Date=2022-09-02
                --abdbsdbcbjc.prquet

        '''

        if len(df.columns) == 2:
            df['sensor'] = sensors[0]
        path = str(self.cpath)
        dir = os.listdir(path)

        if sensors == None:
            raw_metadata = DataAccess.get_device_metadata(self, device_id)
            sensor_spec = 'sensors'
            header = ['sensorId', 'sensorName']
            para_value_list = []
            sen_spec_data_collect = []
            sensor_param_df = pd.DataFrame(raw_metadata[sensor_spec])
            sensor_list = sensor_param_df['sensorId'].tolist()
        else:
            sensor_list = sensors

        df['Date'] = pd.to_datetime(df['time']).dt.date

        if len(dir) == 0:
            table = pa.Table.from_pandas(df)
            pq.write_to_dataset(
                table,
                root_path=str(self.cpath) + device_id + '/',
                partition_cols=['sensor', 'Date'])
        else:
            table = pa.Table.from_pandas(df)
            pq.write_to_dataset(
                table,
                root_path=str(self.cpath) + device_id + '/',
                partition_cols=['sensor', 'Date'])
        return 'success'

    def partial_query(self,device_id,sensors,nocache,df_from_cache,echo=True):
        '''

        :param device_id: 'ABCD'
        :param sensors: 'AB10'
        :param nocache:
        :param df_from_cache:
        :param echo: shows some message to user while data is fetched
        :return: dataframe

        For example: we need to fetch data from 1st November 2022 to 15th January 2023 of TPEM_B3 sensor D29,D30
        In cached we have Data from 1st December 2022 to 31st December 2022 of sensor D29 and D30.

        Partial query function will break the incoming query in two parts.
        - 1st query will fetch data from 1st November 2022 to 30th November using api (data_query function)
        - 2nd query will fetch data from 1st January 2023 to 15th January 2023 using api (data_query function)
        - The remaining data from 1st December 2022 to 31st December 2022 is fetched from cache

        Once data is fetched using api it is simultaneously saved in local cache
        '''

        df = pd.DataFrame()
        rawdata_res = []
        start_list = []
        end_list = []
        # added_time = DataAccess.get_device_metadata(self, device_id)
        year_list = [datetime.strptime(i, '%Y-%m-%d').year for i in nocache]
        format_of_df = {'nocache': nocache, "year": year_list}
        df_nocache = pd.DataFrame(format_of_df)
        # print(df_nocache)
        unique_year_list = list(df_nocache['year'].unique())
        for y in unique_year_list:
            df_upd = list(df_nocache[(df_nocache["year"] == y)]['nocache'])
            start_time = min(df_upd)
            end_time = max(df_upd)
            start_list.append(start_time)
            end_list.append(end_time)
        dict_df = {"start":start_list,"end":end_list}
        df_new = pd.DataFrame(dict_df)
        for i in range(0,(len(df_new))):
            qq = pd.DataFrame()
            start_time = (df_new['start'].iloc[i])
            end_time = (df_new['end'].iloc[i])
            s_time = pd.to_datetime(start_time)
            st_time = int(round(s_time.timestamp())) * 1000000000
            e_time = pd.to_datetime(end_time)
            en_time = int(round(e_time.timestamp())) * 1000000000
            header = {'apikey': self.apikey}
            payload = {}

            if sensors is None:
                url = "https://" + self.url + "/api/apiLayer/getDataByStEt?device="
            else:
                if len(sensors) == 1:
                    url = "https://" + self.url + "/api/apiLayer/getData?device="
                else:
                    url = "https://" + self.url + "/api/apiLayer/getAllData?device="

            a = 0
            cursor = {'start': st_time, 'end': en_time}
            while True:
                if echo == True:
                    for i in range(a):
                        sys.stdout.write('\r')
                        sys.stdout.write("Approx Records Fetched %d" % (10000 * i))
                        sys.stdout.flush()
                if sensors is None:
                    if a == 0:
                        temp = url + device_id + "&sTime=" + str(st_time) + "&eTime=" + str(
                            en_time) + "&cursor=true"
                    else:
                        temp = url + device_id + "&sTime=" + str(cursor['start']) + "&eTime=" + str(
                            cursor['end']) + "&cursor=true"
                if sensors != None:
                    if a == 0:
                        str1 = ","
                        sensor_values = str1.join(sensors)
                        temp = url + device_id + "&sensor=" + sensor_values + "&sTime=" + str(
                            st_time) + "&eTime=" + str(en_time) + "&cursor=true"
                    else:
                        str1 = ","
                        sensor_values = str1.join(sensors)
                        temp = url + device_id + "&sensor=" + sensor_values + "&sTime=" + str(
                            cursor['start']) + "&eTime=" + str(cursor['end']) + "&cursor=true"

                response = requests.request("GET", temp, headers=header, data=payload)
                raw = json.loads(response.text)
                if response.status_code != 200:
                    raise ValueError(raw['error'])
                if 'success' in raw:
                    raise ValueError(raw['error'])
                if len(json.loads(response.text)['data']) == 0:
                    raise ValueError('No Data!')

                else:
                    rawData = json.loads(response.text)['data']
                    cursor = json.loads(response.text)['cursor']
                    rawdata_res = rawdata_res + rawData
                    a = a + 1
                    qq = pd.DataFrame(rawdata_res)

                if cursor['start'] == None or cursor['end'] == None:
                    break
            df = pd.concat([df_from_cache,qq])
            qq.sort_values("time", inplace=True)
            qq.reset_index(drop=True, inplace=True)
            data_dummy = DataAccess.store_data_in_cache(self, device_id, qq, sensors)
        df.sort_values("time", inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df

    def get_sensor_alias(self, device_id, df):
        '''

        :param device_id:
        :param df:
        :return: dataframe with columns having sensor alias

        Maps sensor_alias/ sensor name with corresponding sensor ID
        replaces column names with sensor_alias_sensor_id

        '''
        list1 = list(df['sensor'].unique())
        raw_metadata = DataAccess.get_device_metadata(self, device_id)
        sensor_spec = 'sensors'
        header = ['sensorId', 'sensorName']
        para_value_list = []
        sen_spec_data_collect = []
        list2 = []
        sensor_param_df = pd.DataFrame(raw_metadata[sensor_spec])
        for i in list1:
            sensor_param_df1 = sensor_param_df[sensor_param_df['sensorId'] == i]
            sname = sensor_param_df1.iloc[0]['sensorName']
            sname = sname + " (" + i + ")"
            df['sensor'] = df['sensor'].replace(i, sname)
        return df

    def get_caliberation(self, device_id, qq):
        '''

        :param device_id:
        :param qq:
        :return: Caliberated dataframe

        Perform caliberation on original data
             y = mx + c
             if y is greater than max value replace y with max value
             if y is less than min value replace y with min value

        '''

        sensor_param_df = pd.DataFrame()
        qq.sort_values("sensor", inplace=True)
        raw_metadata = DataAccess.get_device_metadata(self, device_id)
        sensor_spec = 'params'
        header = ['sensorID', 'm', 'c', 'min', 'max', 'automation']
        para_value_list = []
        sen_spec_data = raw_metadata[sensor_spec].keys()
        sen_spec_data_collect = []
        for sensor in sen_spec_data:
            m = c = min_ = max_ = automation = 0
            for name_value in raw_metadata[sensor_spec][sensor]:
                if name_value['paramName'] == 'm':
                    m = name_value['paramValue']
                if name_value['paramName'] == 'c':
                    c = name_value['paramValue']
                if name_value['paramName'] == 'min':
                    min_ = name_value['paramValue']
                if name_value['paramName'] == 'max':
                    max_ = name_value['paramValue']
                if name_value['paramName'] == 'automation':
                    automation = name_value['paramValue']
            sen_spec_data_collect.append([sensor, m, c, min_, max_, automation])
        sensor_param_df = pd.DataFrame(sen_spec_data_collect, columns=header)

        sensor_data_with_meta = qq.merge(sensor_param_df, left_on='sensor', right_on='sensorID').drop('sensor', axis=1)
        if sensor_data_with_meta['c'].values[0] == 0 and sensor_data_with_meta["max"].values[0] == 0 and \
                sensor_data_with_meta["min"].values[0] == 0:
            sensor_data_with_meta['final_value'] = sensor_data_with_meta["value"]
        else:
            # print(sensor_data_with_meta["value"])
            sensor_data_with_meta["value"] = sensor_data_with_meta["value"].replace('BAD 255', '-99999').replace('-',
                                                                                                                 '99999').replace(
                'BAD undefined', '-99999').replace('BAD 0', '-99999')
            sensor_data_with_meta["value"] = sensor_data_with_meta["value"].astype('float')
            sensor_data_with_meta["m"] = sensor_data_with_meta["m"].astype('float')
            sensor_data_with_meta["c"] = sensor_data_with_meta["c"].astype('int')
            # sensor_data_with_meta["max"]  = sensor_data_with_meta["max"].replace('100000000000','009999')
            if type(sensor_data_with_meta["max"][0]) != int:
                sensor_data_with_meta["max"] = sensor_data_with_meta["max"].astype(float)
            sensor_data_with_meta["min"] = sensor_data_with_meta["min"].astype('int')
            # print(sensor_data_with_meta["value"],type((sensor_data_with_meta["value"].values[0])))
            # abssensor_data_with_meta["value"] = sensor_data_with_meta["value"].astype('str')
            sensor_data_with_meta["value"] = sensor_data_with_meta["value"].astype('float')

            # print(sensor_data_with_meta["value"],,type((sensor_data_with_meta["value"].values[0])))

            sensor_data_with_meta['final_value'] = (sensor_data_with_meta['value'] * sensor_data_with_meta['m']) + \
                                                   sensor_data_with_meta['c']

            # sensor_data_with_meta['final_value'] = sensor_data_with_meta['final_value'].astype('float')
            sensor_data_with_meta['final_value'] = np.where(
                sensor_data_with_meta["final_value"] > sensor_data_with_meta["max"], sensor_data_with_meta["max"],
                sensor_data_with_meta["final_value"]
            )
            sensor_data_with_meta['final_value'] = np.where(
                sensor_data_with_meta["final_value"] < sensor_data_with_meta["min"], sensor_data_with_meta["min"],
                sensor_data_with_meta["final_value"]
            )
        df = sensor_data_with_meta[['time', 'final_value', 'sensorID']]
        df.rename(columns={'time': 'time', 'final_value': 'value', 'sensorID': 'sensor'}, inplace=True)
        return df

    def time_grouping(self, df, bands):
        df['Time'] = pd.to_datetime(df['time'])
        df.sort_values("Time", inplace=True)
        df = df.drop(['time'], axis=1)
        df = df.set_index(['Time'])
        df.index = pd.to_datetime(df.index)
        df = df.groupby(pd.Grouper(freq=str(bands) + "Min", label='right')).mean()
        df.reset_index(drop=False, inplace=True)
        return df

    def get_cleaned_table(self, df):
        df = df.sort_values('time')
        df.reset_index(drop=True, inplace=True)
        # df = df.drop_duplicates(keep=First)
        results = df.pivot(index='time', columns='sensor', values='value')
        results.reset_index(drop=False, inplace=True)
        return results

    def get_device_details(self):
        '''

        :return: Details Device id and Device Name of a particular account

        '''
        try:
            qq = pd.DataFrame()
            url = "https://" + self.url + "/api/metaData/allDevices"
            header = {'apikey': self.apikey}
            payload = {}
            response = requests.request('GET', url, headers=header, data=payload, verify=False)

            if response.status_code != 200:
                raw = json.loads(response.text)
                raise ValueError(raw['error'])
            else:
                rawData = json.loads(response.text)['data']
                qq = pd.DataFrame(rawData)
                return qq

        except Exception as e:
            print('Failed to fetch device Details')
            print(e)

    def get_device_metadata(self, device_id):
        '''

        :param device_id: a unique alphanumeric value
        :return: Every detail related to a particular device like device added date, m,c,min,max values, sensor id and its corresponding sensor alias name.

        '''
        try:
            qq = pd.DataFrame()
            url = "https://" + self.url + "/api/metaData/device/" + device_id
            header = {'apikey': self.apikey}
            payload = {}
            response = requests.request('GET', url, headers=header, data=payload, verify=False)

            if response.status_code != 200:
                raw = json.loads(response.text)
                raise ValueError(raw['error'])
            else:
                rawData = json.loads(response.text)['data']
                return rawData

        except Exception as e:
            print('Failed to fetch device Metadata')
            print(e)

    def get_userinfo(self):
        '''
        :return: Details like phone,name,gender,emailid etc
        '''
        try:
            qq = pd.DataFrame()
            url = "https://" + self.url + "/api/metaData/user"
            header = {'apikey': self.apikey}
            payload = {}
            response = requests.request('GET', url, headers=header, data=payload, verify=False)

            if response.status_code != 200:
                raw = json.loads(response.text)
                raise ValueError(raw['error'])
            else:
                rawData = json.loads(response.text)['data']
                return rawData

        except Exception as e:
            print('Failed to fetch user Information')
            print(e)

    def get_dp(self, device_id, sensors=['D29'], n=1, cal=True, end_time=datetime.now()):
        '''

        :param device_id: alphanumeric keyword
        :param sensors: list of sensors
        :param n: number of data points
        :param cal: True/False
        :param end_time: 'year-month-day hours:mins:seconds'
        :return: Last n datapoints from given end_time

        '''
        try:
            frameCount = 0
            lim = n
            sub_list = []
            e_time = pd.to_datetime(end_time)
            en_time = int(round(e_time.timestamp()))
            header = {'apikey': self.apikey}

            qq = pd.DataFrame()
            cursor = {'end': en_time}

            if n == 1:
                if len(sensors) == 1:
                    url = "https://" + self.url + "/api/apiLayer/getLimitedDataMultipleSensors/?device=" + device_id + "&sensor=" + \
                          sensors[0] + "&eTime=" + str(
                        en_time) + "&lim=" + str(lim) + "&cursor=true"
                    payload = {}
                    param = 'GET'
                    response = requests.request(param, url, headers=header, data=payload, verify=False)
                    raw = json.loads(response.text)
                    if response.status_code != 200:
                        raise ValueError(raw['error'])
                    if 'success' in raw:
                        raise ValueError(raw['error'])
                    if len(json.loads(response.text)) == 0:
                        raise ValueError('No Data!')
                    else:
                        rawData = json.loads(response.text)['data']
                        qq = pd.DataFrame(rawData)
                        if cal == True or cal == 'true' or cal == "TRUE":
                            df = DataAccess.get_caliberation(self, device_id, qq)
                            df = DataAccess.get_sensor_alias(self, device_id, df)
                        else:
                            df = qq
                            df = DataAccess.get_sensor_alias(self, device_id, df)
                    return df

                else:
                    payload = {"sensors": sensors}
                    url = "https://" + self.url + "/api/apiLayer/getLastDps?device=" + device_id
                    param = 'PUT'
                    response = requests.request(param, url, headers=header, data=payload, verify=False)
                    raw = json.loads(response.text)
                    if response.status_code != 200:
                        raise ValueError(raw['error'])
                    if 'success' in raw:
                        raise ValueError(raw['error'])
                    elif len(json.loads(response.text)) == 0:
                        raise ValueError('No Data!')
                    else:
                        time_list = []
                        raw = json.loads(response.text)
                        for i in raw[0]:
                            filtered = raw[0][i]
                            sub_list = sub_list + filtered

                        qq = pd.DataFrame(sub_list)
                        print('qq', qq)
                        if cal == True or cal == 'true' or cal == "TRUE":
                            df = DataAccess.get_caliberation(self, device_id, qq)
                            df = DataAccess.get_sensor_alias(self, device_id, df)
                            df = df.sort_values('time')
                            df = df.pivot(index='time', columns='sensor', values='value')
                            df.reset_index(drop=False, inplace=True)
                        else:
                            df = DataAccess.get_sensor_alias(self, device_id, qq)
                            df = df.sort_values('time')
                            df = df.pivot(index='time', columns='sensor', values='value')
                            df.reset_index(drop=False, inplace=True)
                        return df
            else:
                str1 = ","
                sensor_values = str1.join(sensors)
                payload = {}
                a = 0
                while True:
                    if a == 0:
                        url = "https://" + self.url + "/api/apiLayer/getLimitedDataMultipleSensors/?device=" + device_id + "&sensor=" + sensor_values + "&eTime=" + str(
                            en_time) + "&lim=" + str(lim) + "&cursor=true"
                    else:
                        url = "https://" + self.url + "/api/apiLayer/getLimitedDataMultipleSensors/?device=" + device_id + "&sensor=" + sensor_values + "&eTime=" + str(
                            cursor['end']) + "&lim=" + str(lim) + "&cursor=true"
                    response = requests.request("GET", url, headers=header, data=payload)
                    raw = json.loads(response.text)
                    if response.status_code != 200:
                        raise ValueError(raw['error'])
                    if 'success' in raw:
                        raise ValueError(raw['error'])
                    if len(json.loads(response.text)['data']) == 0:
                        raise ValueError('No Data!')
                    else:
                        rawData = json.loads(response.text)['data']
                        cursor = json.loads(response.text)['cursor']
                        a = a + 1
                        qq = pd.DataFrame(rawData)

                    if cursor['end'] == None:
                        break
                if cal == True or cal == 'true' or cal == "TRUE":
                    df = DataAccess.get_caliberation(self, device_id, qq)
                    df = DataAccess.get_sensor_alias(self, device_id, df)
                    df = DataAccess.get_cleaned_table(self, df)
                else:
                    df = DataAccess.get_sensor_alias(self, device_id, qq)
                    df = DataAccess.get_cleaned_table(self, df)
            return df
        except Exception as e:
            print(e)

    def data_query(self, device_id, start_time, end_time=time.time(), sensors=None, cal=None, bands=None, echo=True):
        try:
            sens = []
            dictionary = {}
            df = pd.DataFrame()
            qq = pd.DataFrame()
            rawdata_res = []
            msg, incache, nocache, sensor_list = DataAccess.check_in_cache(self, device_id, sensors, start_time,end_time)
            if len(nocache) != 0 and msg == 'yes':
                df = DataAccess.get_data_from_cache(self, incache, sensor_list, device_id)
                df = DataAccess.partial_query(self,device_id,sensors,nocache,df,echo=True)
            if len(nocache) == 0 and msg == 'yes':
                df = DataAccess.get_data_from_cache(self, incache, sensor_list, device_id)
                df.reset_index(drop=True, inplace=True)
                df.sort_values("time", inplace=True)
                df.reset_index(drop=True, inplace=True)

            if len(incache) == 0 and msg != 'yes' and len(nocache) != 0:
                s_time = pd.to_datetime(start_time)
                st_time = int(round(s_time.timestamp())) * 1000000000
                e_time = pd.to_datetime(end_time)
                en_time = int(round(e_time.timestamp())) * 1000000000
                header = {'apikey': self.apikey}
                payload = {}

                if sensors is None:
                    url = "https://" + self.url + "/api/apiLayer/getDataByStEt?device="
                else:
                    if len(sensors) == 1:
                        url = "https://" + self.url + "/api/apiLayer/getData?device="
                    else:
                        url = "https://" + self.url + "/api/apiLayer/getAllData?device="

                a = 0
                cursor = {'start': st_time, 'end': en_time}

                while True:
                    if echo == True:
                        for i in range(a):
                            sys.stdout.write('\r')
                            sys.stdout.write("Approx Records Fetched %d" % (10000 * i))
                            sys.stdout.flush()
                    if sensors is None:
                        if a == 0:
                            temp = url + device_id + "&sTime=" + str(st_time) + "&eTime=" + str(
                                en_time) + "&cursor=true"
                        else:
                            temp = url + device_id + "&sTime=" + str(cursor['start']) + "&eTime=" + str(
                                cursor['end']) + "&cursor=true"
                    if sensors != None:
                        if a == 0:
                            str1 = ","
                            sensor_values = str1.join(sensors)
                            temp = url + device_id + "&sensor=" + sensor_values + "&sTime=" + str(
                                st_time) + "&eTime=" + str(en_time) + "&cursor=true"
                        else:
                            str1 = ","
                            sensor_values = str1.join(sensors)
                            temp = url + device_id + "&sensor=" + sensor_values + "&sTime=" + str(
                                cursor['start']) + "&eTime=" + str(cursor['end']) + "&cursor=true"

                    response = requests.request("GET", temp, headers=header, data=payload)
                    raw = json.loads(response.text)
                    if response.status_code != 200:
                        raise ValueError(raw['error'])
                    if 'success' in raw:
                        raise ValueError(raw['error'])
                    if len(json.loads(response.text)['data']) == 0:
                        raise ValueError('No Data!')
                    else:
                        rawData = json.loads(response.text)['data']
                        # print('Raw Data Value',rawData)
                        cursor = json.loads(response.text)['cursor']
                        rawdata_res = rawdata_res + rawData
                        a = a + 1
                        qq = pd.DataFrame(rawdata_res)

                    if cursor['start'] == None or cursor['end'] == None:
                        break
                df = qq
                DataAccess.store_data_in_cache(self, device_id, df, sensors)

            if len(df.columns) == 2:
                df['sensor'] = sensors[0]

            if cal == True or cal == 'true' or cal == "TRUE":
                # #qq=qq[~qq['value'].isin(['','BAD 255','BAD 0','BAD undefined','-'])]
                df = DataAccess.get_caliberation(self, device_id, df)
                df = DataAccess.get_sensor_alias(self, device_id, df)
                df = DataAccess.get_cleaned_table(self, df)
            else:
                df = DataAccess.get_sensor_alias(self, device_id, df)
                df = DataAccess.get_cleaned_table(self, qq)

            if bands != None:
                df = DataAccess.time_grouping(self, df, bands)
            return df

        except Exception as e:
            # print('Failed to fetch Data')
            print(e)

    def publish_event(self, title, message, metaData, hoverData, eventTags, created_on):
        rawData = []
        try:
            url = "https://" + self.url + "/api/eventTag/publishEvent"
            header = {'apikey': self.apikey}
            payload = {
                "title": title,
                "message": message,
                "metaData": metaData,
                "eventTags": [eventTags],
                "hoverData": "",
                "createdOn": created_on
            }
            response = requests.request('POST', url, headers=header, json=payload, verify=True)

            if response.status_code != 200:
                raw = json.loads(response.text)
                raise ValueError(raw['error'])
            else:
                rawData = json.loads(response.text)['data']
                return rawData

        except Exception as e:
            print('Failed to fetch event Details')
            print(e)

        return rawData

    def get_events_in_timeslot(self, start_time, end_time):
        try:
            url = "https://" + self.url + "/api/eventTag/fetchEvents/timeslot"
            header = {'apikey': self.apikey}
            payload = {
                "startTime": start_time,
                "endTime": end_time
            }
            response = requests.request('PUT', url, headers=header, data=payload, verify=False)

            if response.status_code != 200:
                raw = json.loads(response.text)
                raise ValueError(raw['error'])
            else:
                rawData = json.loads(response.text)['data']
                return rawData

        except Exception as e:
            print('Failed to fetch event Details')
            print(e)

        return rawData

    def get_event_data_count(self, end_time=time.time(), count=None):
        try:
            url = "https://" + self.url + "/api/eventTag/fetchEvents/count"
            header = {'apikey': self.apikey}
            payload = {
                "endTime": end_time,
                "count": count
            }
            response = requests.request('PUT', url, headers=header, json=payload, verify=False)

            if response.status_code != 200:
                raw = json.loads(response.text)
                raise ValueError(raw['error'])
            else:
                rawData = json.loads(response.text)
                return rawData

        except Exception as e:
            print('Failed to fetch event Count')
            print(e)

        return rawData

    def get_event_categories(self):
        try:
            url = "https://" + self.url + "/api/eventTag"
            header = {'apikey': self.apikey}
            payload = {}
            response = requests.request('GET', url, headers=header, data=payload, verify=False)

            if response.status_code != 200:
                raw = json.loads(response.text)
                raise ValueError(raw['error'])
            else:
                rawData = json.loads(response.text)['data']
                return rawData

        except Exception as e:
            print('Failed to fetch event Count')
            print(e)

        return rawData
