# Pollen_map_japan
Draw the pollen distribution map and make the movie

## Sample

![sample](https://github.com/takakiba/Pollen_map_japan/blob/main/Pollen_map_japan_2023_cropped.gif)

## Required libraries
- requests
- geopandas
- pandas
- h5py
- numpy
- opencv

## How to run
1. install the required libraries
2. get the geometry data of cities at https://nlftp.mlit.go.jp/ksj/jpgis/datalist/KsjTmplt-N03.html
3. run `python collect_data.py` first to update pollen data from online
4. run `python draw_pollen_map.py` next to draw pollen distribution map
5. run `python makemovie.py` to output movie of time history of pollen distribution

## References
- https://wxtech.weathernews.com/pollen/index.html
- https://note.nkmk.me/python-unix-time-datetime/
- https://qiita.com/keisuke0508/items/df2594770d63bf124ccd

