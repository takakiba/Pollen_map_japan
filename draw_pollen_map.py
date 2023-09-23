import geopandas as gpd
import datetime
import matplotlib.pyplot as plt
import matplotlib.colors as mplcolors
import matplotlib.cm as mplcm
import numpy as np
import os
import h5py
import pandas as pd
import tqdm


target_year = 2023
# prefectures = ['宮城県', '山形県']
prefectures = []
geometry_file = 'N03-200101_GML/N03-20_200101.shp'
hdf5_database = 'pollen_data_japan_plot.h5'


def extract_gdf(geo_file, prefecture_list):
    '''
    extract geopandas dataframe from geometry file
    returns geopandas dataframe and list of city codes to plot
    '''

    ### read geometry file with geopandas
    gdf_jp = gpd.read_file(geo_file, encoding='cp932')

    if prefecture_list:
        ### if prefectures are given, the geopandas dataframe of selected prefectures are extracted
        gdf_list = [gdf_jp[ gdf_jp['N03_001'] == p ] for p in prefecture_list]
        gdf = gpd.GeoDataFrame(pd.concat(gdf_list, ignore_index=True))
    else:
        ### if prefecture list is empty, use whole data
        gdf = gdf_jp

    ### list up city codes in geopandas dataframe
    cc_list = []
    for cc in gdf['N03_007']:
        if cc not in cc_list:
            cc_list.append(cc)

    return gdf, cc_list


def get_plot_duration(year, city_codes, hdf5_database):
    '''
    check oldest and latest date and maximum pollen data of data in database
    '''

    ### prepare timezone as japan
    timezone = datetime.timezone(datetime.timedelta(hours=9))
    ### timestamps and maximum pollen containers
    timestamp0 = -1e200
    timestamp1 = 1e200
    max_pol = 0

    print('Serch begin and end date')

    with h5py.File(hdf5_database, 'r') as f5:
        ### loop for city codes
        for cc in tqdm.tqdm(city_codes):
            ### load all the data of the city
            date_dset = f5['{0}/{1}/Date'.format(year, cc)][()]
            poll_dset = f5['{0}/{1}/Pollen'.format(year, cc)][()]
            ts0 = np.min(date_dset)
            ts1 = np.max(date_dset)

            ### update the beginning of plot date with latest date
            if ts0 > timestamp0:
                timestamp0 = ts0
            ### update the end of plot date with oldest date
            if ts1 < timestamp1:
                timestamp1 = ts1
            ### update maximum pollen data
            if max_pol < np.max(poll_dset):
                max_pol = np.max(poll_dset)
        ### convert the date info from unix time to datetime object
        dt0 = datetime.datetime.fromtimestamp(timestamp0, tz=timezone)
        dt1 = datetime.datetime.fromtimestamp(timestamp1, tz=timezone)
    return dt0, dt1, max_pol


def get_pollen_data(year, datehour, city_codes, hdf5_database):
    '''
    collect data on the specified date and hour for multiple cities given by 'city_codes' list
    '''

    ### convert datetime object to timestamp to compare with the data in hdf5 file
    target_ts = datehour.timestamp()
    ### prepare pollen data array of the same size as that of city_codes
    pol_array = np.zeros(len(city_codes), dtype=np.int64)

    i = 0

    with h5py.File(hdf5_database, 'r') as f5:
        ### loop for citiew
        for cc in city_codes:
            ### retrieve the date data as unix time
            date_dset = f5['{0}/{1}/Date'.format(year, cc)][()]

            ### get index of specified date
            idx = np.where(date_dset == target_ts)

            ### get pollend data of specified date
            pol_data = f5['{0}/{1}/Pollen'.format(year, cc)][idx]

            ### contain the pollen data in array
            pol_array[i] = pol_data

            i += 1

    ### prepare return data as pandas series
    ser_pol = pd.Series(
            data = pol_array,
            index = city_codes
            )

    return ser_pol



def plot_pollen_map(year, gdf, city_codes, hdf5_database):
    '''
    plot maps based on hdf5 database
    '''

    ### directory to save drawn map
    png_dir = 'pollen_map_japan_png'
    if not os.path.isdir(png_dir): os.makedirs(png_dir)

    ### get the datetime of beginning and end and maximum pollen data
    dt0, dt1, max_pol = get_plot_duration(year, city_codes, hdf5_database)

    print('Plot range {0:%Y-%m-%d} - {1:%Y-%m-%d}'.format(dt0, dt1))

    ### variable of current datetime of plot
    plot_dt = dt0

    ### prepare campus for plot
    fig = plt.figure(figsize=(12, 12))
    ### use jet for coloring
    cmap = mplcm.jet
    ### norm with maximum pollen data
    norm = mplcolors.Normalize(vmin=0, vmax=max_pol)

    while plot_dt <= dt1:
        ### define the png file name
        png_filename = png_dir + '/PollenMap_{0:%Y-%m-%d_%H%M}.png'.format(plot_dt)

        if not os.path.isfile(png_filename):
            ### if there is no file, drawing start
            print('\rPlot on {0:%Y-%m-%d %H:%M}'.format(plot_dt), end='')
            ### obtain the pollen data of the current date and hour
            ser_pol = get_pollen_data(year, plot_dt, city_codes, hdf5_database)

            ### prepare list for coloring based on pollen number
            fc = [ ser_pol[cc] for cc in gdf['N03_007'] ]

            ### prepare axis
            ax = fig.add_subplot(111)
            ax.axis('off')
            ax.set_aspect('equal')

            ### draw map distribution
            gdf.plot(
                    ax = ax,
                    edgecolor = 'black',
                    facecolor = mplcm.ScalarMappable(norm=norm, cmap=cmap).to_rgba(fc),
                    linewidth = 0.1
                    )
            ### put colorbar
            fig.colorbar(
                    mplcm.ScalarMappable(norm=norm, cmap=cmap), 
                    orientation = 'vertical',
                    fraction = 0.05,
                    aspect = 50
                    )
            ### format the final apperance of map
            fig.suptitle('{0:%Y-%m-%d %H:%M}'.format(plot_dt))
            plt.tight_layout()
            # plt.show()
            plt.savefig(png_filename)
            plt.clf()

        ### iteration for next datetime (one hour step)
        plot_dt += datetime.timedelta(hours=1)

    print('')


def plot_gdf_random_patch(gdf):
    '''
    just for debug and demonstrate how to draw map
    '''
    fig = plt.figure(figsize=(12, 12))

    ax = fig.add_subplot(111)
    ax.axis('off')
    ax.set_aspect('equal')

    fc = np.random.randint(-1, high = 255, size = len(gdf))

    cmap = mplcm.jet
    norm = mplcolors.Normalize(vmin=0, vmax=np.max(fc))

    gdf.plot(
            ax = ax,
            edgecolor = 'black',
            facecolor = mplcm.ScalarMappable(norm=norm, cmap=cmap).to_rgba(fc),
            linewidth = 0.1
            )
    fig.colorbar(
            mplcm.ScalarMappable(norm=norm, cmap=cmap), 
            orientation = 'vertical',
            fraction = 0.05,
            aspect = 50
            )
    fig.suptitle('Sample Japan')
    plt.tight_layout()
    plt.show()


def draw_pollen_map(year, geo_file, prefectures, h5_database):
    '''
    read geometry data and call main drawing loop function
    '''
    ### read geometry data to obtain geopandas dataframe and list of citycodes of specified prefectures
    gdf, city_codes = extract_gdf(geo_file, prefectures)
    # plot_gdf_random_patch(gdf)

    ### call function to draw pollen maps
    plot_pollen_map(year, gdf, city_codes, hdf5_database)


if __name__ == '__main__':
    draw_pollen_map(target_year, geometry_file, prefectures, hdf5_database)

