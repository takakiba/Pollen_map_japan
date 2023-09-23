import geopandas as gpd
import requests
import pandas as pd
import io
import datetime
import calendar
import h5py
import os
import shutil
import numpy as np
import time

# prefectures = ['北海道']
# prefectures = ['宮城県']
prefectures = []

collect_start = datetime.date(2023, 2,  1)
collect_end   = datetime.date(2023, 5, 31)

geometry_file = 'N03-200101_GML/N03-20_200101.shp'
hdf5_database = 'pollen_data_japan.h5'


def listup_city_code(gdf_jp, prefectures):
    '''
    list up city code from geometry data

    Input:
        - gdf_jp : geopandas data to read
        - prefectures : list of prefectures to list up city code,
                        if empty, list up all city codes in japan
    Output:
        - list of city codes in specified prefectures
    '''
    ### list of city code list to return
    cc_list = []

    if prefectures:
        ### if prerectures are specified
        ### loop for prefectures
        for p in prefectures:
            ### extract geopandas dataframe with one prefecture
            gdf = gdf_jp[ gdf_jp['N03_001'] == p ]
            for cc in gdf['N03_007']:
                ### add city code if not in the list yet
                if cc not in cc_list:
                    cc_list.append(cc)
    else:
        ### the same loop above for all prefectures
        for cc in gdf_jp['N03_007']:
            if cc not in cc_list:
                cc_list.append(cc)

    return cc_list


def create_month_list(start_date, end_date):
    '''
    Create list of days in the month
    '''
    ### count number of months
    num_month = (end_date.month - start_date.month) + 1 

    month_list = []

    date0 = start_date
    ### loop for monthly list
    for i in range(num_month):
        ### get number of days in month
        d0, d1 = calendar.monthrange(date0.year, date0.month)

        ### final date of the month
        date1 = datetime.date(date0.year, date0.month, d1)

        ### update final date if date1 is over
        if date1 > end_date: date1 = end_date

        month_list.append([date0, date1])

        ### update the biginning of month
        date0 = date1 + datetime.timedelta(days=1)
    return month_list


def get_data(start:str, end:str, citycode='04103'):
    ### get pollen date from wethernews api in one month

    url = 'https://wxtech.weathernews.com/opendata/v1/pollen?'
    payload = {"citycode": citycode, "start": start, "end": end}
    r = requests.get(url, params=payload)

    # print(citycode)

    if r.ok:
        ### if request is success, prepare dataframe to return
        ### convert to dataframe
        df_temp = pd.read_csv(
            io.StringIO(r.text), 
            sep=',',
            dtype={'citycode': str, 'date': str, 'pollen': int}
        )

        df = pd.DataFrame({
            'Date' : df_temp['date'],
            citycode: df_temp['pollen']
            })
        ### convert datetime text to datetime type
        df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%dT%H:%M:%S%z')

        return df

    else:
        ### if request is failed, return '-1' data for all the dates as dummy dataframe
        end_point = datetime.datetime.strptime(end + ' 23:00:00', '%Y%m%d %H:%M:%S')
        ser_date = pd.Series(
                pd.date_range(
                    start, 
                    end=end_point, 
                    freq='H', 
                    tz='Asia/Tokyo'), 
                name='Date'
                )
        ser_pollen = pd.Series(data=np.ones(len(ser_date), dtype=np.int64)*-1, name=citycode)
        df = pd.concat([ser_date, ser_pollen], axis=1)
        return df


def get_city_pollen_data(start_date, end_date, city_code):
    '''
    collect pollen date during specified term between start_date and end_date in one city
    '''

    month_list = create_month_list(start_date, end_date)

    df = pd.DataFrame()

    ### get monthly data
    for ds in month_list:
        print('{0:%Y%m%d} - {1:%Y%m%d}'.format(ds[0], ds[1]))

        df_c = get_data(
                '{0:%Y%m%d}'.format(ds[0]), 
                '{0:%Y%m%d}'.format(ds[1]), 
                citycode=city_code
            )

        df = pd.concat([df, df_c], axis=0)
    return df


def update_city_data(date_start, date_end, city_code, hdf5_file):
    ### retrieve year for year group in hdf5 file
    year = date_start.year

    with h5py.File(hdf5_file, 'a') as f5:
        ### create group of city
        city_grp = f5.require_group('{0}/{1}'.format(year, city_code))

        ### create dataset for pollen data
        pol_dset = city_grp.require_dataset(
                name = 'Pollen',
                shape = (1, ),
                maxshape = (366*24, ),
                dtype = np.int64
                )
        ### create dataset for dates in unix time
        date_dset = city_grp.require_dataset(
                name = 'Date',
                shape = (1, ),
                maxshape=(366*24, ),
                dtype = np.float64
                )

        if len(date_dset) < 2:
            ### if dataset is empty, fetch all the data from online
            print('Fetch data online for {0}'.format(city_code))
            df = get_city_pollen_data(date_start, date_end, city_code)
            # print(df)
        else:
            ### if dataset have data, load local database first
            print('Read data offline for {0}'.format(city_code))

            ### create dataframe, date is converted unix time (float) -> python datetime object
            df = pd.DataFrame({
                'Date' : [
                    datetime.datetime.fromtimestamp(t, tz=datetime.timezone(datetime.timedelta(hours=9))) 
                    for t in date_dset[()]
                    ],
                city_code : pol_dset[()]
                })

            ### check the duration of data in local database
            local_date0 = df['Date'].iloc[0].date()
            local_date1 = df['Date'].iloc[-1].date()

            if date_start < local_date0:
                ### if we need old data, fetch them online
                print('Fetch data online for ', end='')
                df_t = get_city_pollen_data(date_start, local_date0 + datetime.timedelta(days=-1), city_code)
                df = pd.concat([df_t, df], axis=0)
            if local_date1 < date_end:
                ### if we need latest data, fetch them online
                print('Fetch data online for ', end='')
                df_t = get_city_pollen_data(local_date1 + datetime.timedelta(days=1), date_end, city_code)
                df = pd.concat([df, df_t], axis=0)

        ### reindex
        df.reset_index(drop=True, inplace=True)
        ### get number of data
        num_data = len(df)

        ### resize the dataset to save data
        date_dset.resize(num_data, axis=0)
        pol_dset.resize(num_data, axis=0)

        ### update the local database
        date_dset[:num_data] = [d.timestamp() for d in df['Date']]
        pol_dset[:num_data] = df[city_code].values

        # print(df)


def update_database(date_start, date_end, city_list, hdf5_file):
    time_csv = 'time_history.csv'
    with open(time_csv, 'w') as f:
        f.write('i, city code, time\n')
    ### loop for cities to investigate
    for i, cc in enumerate(city_list):
        time0 = time.time()
        update_city_data(date_start, date_end, cc, hdf5_file)
        time1 = time.time() - time0
        with open(time_csv, 'a') as f:
            f.write('{0}, {1}, {2:.4f}\n'.format(i, cc, time1))


if __name__ == '__main__':

    ### load database
    gdf_jp = gpd.read_file(geometry_file, encoding='cp932')

    city_codes = listup_city_code(gdf_jp, prefectures)
    # city_codes = city_codes[40:50]

    update_database(collect_start, collect_end, city_codes, hdf5_database)


