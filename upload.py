import logging
import os
import shutil
import tempfile
from glob import glob
from io import BytesIO
import json

import oss2
import pandas as pd
import rarfile
import geopandas as gpd
from shapely import geos
from shapely.geometry import Point, Polygon
from shapely.geometry.base import BaseGeometry, BaseMultipartGeometry
from shapely.wkb import loads as wkb_loads
from shapely.wkt import loads as wkt_loads




class UploadTaskException(Exception):
  """errors that can be displayed to user"""
  pass

class ValidationError(Exception):
  """errors that can be displayed to user"""
  pass


def load_type(extension):
  extension = extension[1:]
  if extension in ('txt', 'csv', 'xlsx', 'xls'):
    # poi, plain
    task_type = 'poi'
  elif extension in ('zip', 'rar', 'gz'):
    # geometry
    task_type = 'geometry'
  else:
    raise ValidationError('只支持 xls|csv|xlsx|zip|rar|gz')
  return task_type

def geometry_2_ewkb4326(geometry: [BaseGeometry, BaseMultipartGeometry]):
  """Insert srid:4326 into shapely geometry and get the ewkb form

  suppress z dimension
  """
  if geometry.has_z:
    # NOTICE: this lost z dimension info and the method is deprecated
    geometry = wkt_loads(geometry.to_wkt())
  geos.lgeos.GEOSSetSRID(geometry._geom, 4326)
  return geometry.wkb_hex

def point_ewkb4326(lng: float, lat: float):
  point = Point(lng, lat)
  return geometry_2_ewkb4326(point)


def polygon_ewkb4326(points: list):
  polygon = Polygon(points)
  return geometry_2_ewkb4326(polygon)

def change_df_eng_field_to_chi(df: pd.DataFrame) -> pd.DataFrame:
  """翻译文件头"""
  # TODO according to chi/eng bytes to limit column name length
  columns = list(map(lambda x: x[:21], df.columns.astype(str)))
  match = {'name': '名称',
           'type': '类型',
           'address': '地址',
           'lng': '经度',
           'lat': '纬度'}
  df.columns = [match.get(i.lower(), i) for i in columns]
  return df

def df_2_gdf(df: pd.DataFrame):
  """将包含ewkb列的DataFrame转换成GeoDataFrame"""
  if 'geometry' not in df.columns:
    raise UploadTaskException(
        'csv/xlsx使用围栏上传需要包含名为(geometry)的ewkb列')
  if df.geometry.isnull().any():
    raise UploadTaskException(
        'csv/xlsx使用围栏上传时, 列geometry不能含有空值')
  df['geometry'] = df.geometry.apply(lambda x: wkb_loads(x, hex=True))
  return gpd.GeoDataFrame(df, crs={'init': 'epsg:4326'})

def sanity_check(f, base_name, task_type) -> [pd.DataFrame, str]:
  """检查文件有效性"""

  if task_type == 'big_screen':
    return sanity_check_big_screen(f, base_name), task_type
  elif task_type in ('poi', 'plain'):
    try:
      return sanity_check_poi_or_plain(f, base_name), task_type
    except UploadTaskException as e:
      # TODO: a bit hacking
      if e.args[0] == 'use_geometry':
        # f.seek(0)
        return sanity_check_geometry(f, base_name), 'geometry'
      else:
        raise
  elif task_type == 'geometry':
    return sanity_check_geometry(f, base_name), task_type
  else:
    raise UploadTaskException('未知任务类型 {}'.format(task_type))

def sanity_check_big_screen(f, base_name) -> [pd.DataFrame, dict]:
  """big screen data sanity check"""
  extension = os.path.splitext(base_name)[-1]
  if extension == '.csv':
    return read_csv(f)
  elif extension == '.xlsx':
    return read_excel(f)
  else:
    raise UploadTaskException('不支持的文件格式： {}'.format(extension))

def sanity_check_poi_or_plain(f: [BytesIO, str], base_name) -> pd.DataFrame:
  """poi sanity check"""
  base, extension = os.path.splitext(base_name)
  if extension == '.csv':
    df = read_csv(f)
  elif extension == '.xlsx':
    dfs = read_excel(f)
    if len(dfs) == 1:
      # single sheet
      sheet_name, df = dfs.popitem()
    else:
      frames = []
      for sheet_name, frame in sorted(dfs.items(), key=lambda x: x[0]):
        frames.append(frame)
      df = pd.concat(frames)
  else:
    raise UploadTaskException('不支持的文件类型：{}'.format(extension))
  df = change_df_eng_field_to_chi(df)
  # NOTICE: 如果包含geometry, 切换为geometry模式
  if 'geometry' in df.columns:
    raise UploadTaskException('use_geometry')
  df['类型'] = base.split('_', 1)[-1].strip()

  columns_exists = set(df.columns)
  if '名称' not in df.columns:
    raise UploadTaskException(
        '缺失必要的列: 名称\n实际列: {}'.format('|'.join(df.columns.tolist())))

  plain_exclude_columns = {'经度', '纬度', '地址'}
  if len(plain_exclude_columns - columns_exists) == 3:
    # plain
    df['geometry_type'] = 'plain'
    for col in plain_exclude_columns:
      df[col] = None
  else:
    # poi
    columns_lnglat = {'经度', '纬度'}
    if (columns_lnglat - columns_exists) and ('地址' not in columns_exists):
      raise UploadTaskException(
          '缺失必要的列: 需要 (经度, 纬度) 或者 (地址)')
    if '地址' not in columns_exists:
      df['地址'] = None
    else:
      df['地址'] = df['地址'].astype(str).str.strip()
      # TODO Think a more elegant way to deal with 'nan'
      df.loc[df.地址 == 'nan', '地址'] = None
    if '经度' not in columns_exists:
      df['经度'] = None
      df['纬度'] = None
    for col in ['经度', '纬度']:
      try:
        df[col] = df[col].astype(float)
      except ValueError:
        raise UploadTaskException(f'{col} 转换为浮点数失败')
      empty = df.loc[df[col].isnull() & df['地址'].isnull()]
      if not empty.empty:
        raise UploadTaskException(
            '{} 和 地址 不能同时为空, 总数: {}, 行号(前10): {}'.format(
                col,
                empty.shape[0],
                empty.index[:10].tolist()
            ))
    df['geometry_type'] = 'point'

    # force set missing lng / lat to be all nan
    df.loc[df['经度'].isnull() | df['纬度'].isnull(), ['经度', '纬度']] = None
    overflow_lat = df.loc[
      df['纬度'].notnull() & ((df['纬度'] >= 90) | (df['纬度'] <= -90))]
    if not overflow_lat.empty:
      raise UploadTaskException(
          '纬度已超过范围值[-90, 90], 总数: {}, 行号(前10): {}'.format(
              col,
              overflow_lat.shape[0],
              overflow_lat.index[:10].tolist()
          ))
    overflow_lng = df.loc[
      df['纬度'].notnull() & ((df['经度'] >= 180) | (df['经度'] <= -180))]
    if not overflow_lng.empty:
      raise UploadTaskException(
          '纬度已超过范围值[-180, 180], 总数: {}, 行号(前10): {}'.format(
              col,
              overflow_lng.shape[0],
              overflow_lng.index[:10].tolist()
          ))
  for col in ['名称']:
    df[col] = df[col].astype(str).str.strip()
    empty = df.loc[(df[col].str.len() == 0) | (df[col].isnull())]
    if not empty.empty:
      raise UploadTaskException(
          '{} 缺失或为空, 总数: {}, 行号(前10): {}'.format(
              col,
              empty.shape[0],
              '|'.join(empty.index[:10].tolist())))
  return df

def sanity_check_geometry(f: [BytesIO, str], base_name) -> pd.DataFrame:
  """geometry shape 文件 sanity check"""
  base, extension = os.path.splitext(base_name)
  if extension == '.csv':
    df = df_2_gdf(self.read_csv(f))
  elif extension == '.xlsx':
    dfs = read_excel(f)
    if len(dfs) == 1:
      # single sheet
      sheet_name, df = dfs.popitem()
    else:
      frames = []
      for sheet_name, frame in sorted(dfs.items(), key=lambda x: x[0]):
        frames.append(frame)
      df = pd.concat(frames)
    df = df_2_gdf(df)
  elif extension in ['.gz', '.zip', '.rar']:
    df = read_shp(f)
  else:
    raise UploadTaskException('不支持的文件类型： {}'.format(extension))

  if df.empty:
    raise UploadTaskException('文件内容为空')

  df = change_df_eng_field_to_chi(df)
  df['类型'] = base.split('_', 1)[-1].strip()

  columns_required = {'名称', 'geometry'}
  columns_exists = set(df.columns)
  columns_missing = columns_required - columns_exists
  if columns_missing:
    raise UploadTaskException(
        '缺失必要的列: {}, 实际列: {}'.format(
            '|'.join(columns_missing), '|'.join(columns_exists)))
  df['名称'] = df['名称'].astype(str).str.strip()
  df['类型'] = df['类型'].astype(str).str.strip()

  if df.geometry.isnull().any():
    no_geometry = df[df.geometry.isnull()]
    raise UploadTaskException(
        '包含空geometry, 总数: {}, 行号(前10): {}'.format(
            no_geometry.shape[0], no_geometry.index[:10].tolist()))
  df.loc[df.geom_type.isin(['Polygon', 'MultiPolygon']),
         'geometry'] = df['geometry'].buffer(0)
  if not df.is_valid.all():
    invalid = df.loc[~df.is_valid]
    raise UploadTaskException(
        '包含无效geometry, 总数: {}, 行号(前10): {}'.format(
            invalid.shape[0], invalid.index[:10].tolist()))

  proj_info = df.crs.get('init', None)
  if proj_info != 'epsg:4326':
    raise UploadTaskException(
        '地理编码目前只支持 epsg:4326, 文件实际编码为 {}'.format(proj_info))

  non_polygon = df.loc[~df.geom_type.isin(
      ['Point', 'Polygon', 'MultiPolygon', 'LineString', 'MultiLineString'])]
  if not non_polygon.empty:
    raise UploadTaskException(
        '包含非Polygon和MultiPolygon, 总数: {}, 行号(前10): {}'.format(
            non_polygon.shape[0], non_polygon.index[:10].tolist()))

  return df

def read_csv(f: BytesIO) -> pd.DataFrame:
  """读取csv, 自适应编码"""
  try:
    # TODO: check index col
    try:
      df = pd.read_csv(f)
    except UnicodeDecodeError:
      # f.seek(0)
      df = pd.read_csv(f, encoding='gb18030')
  except Exception as e:
    logging.warning(e)
    raise UploadTaskException('文件读取错误, 请检查文件格式是否为csv')
  else:
    return df

def read_excel(f: BytesIO) -> dict:
  """读取excel, 自适应编码"""
  try:
    try:
      dfs = pd.read_excel(f, sheet_name=None)
    except UnicodeDecodeError:
      # f.seek(0)
      dfs = pd.read_excel(f, encoding='gb18030', sheet_name=None)
  except Exception as e:
    logging.warning(e)
    raise UploadTaskException('文件读取错误, 请检查文件格式是否为xlsx')
  else:
    if not dfs:
      raise UploadTaskException('文件不包含有效的sheet')
    return dfs

def read_shp(f: str) -> gpd.GeoDataFrame:
  """读取shp, 自适应编码"""
  try:
    try:
      # TODO: may use chardet on .dbf
      df = gpd.read_file(f)
    except UnicodeDecodeError:
      df = gpd.read_file(f, encoding='utf8')
  except Exception as e:
    logging.warning(e)
    raise UploadTaskException('文件读取错误, 请检查文件格式是否为shp')
  else:
    return df

def data_processing(df, task_type, object_type):
  """处理文件内容"""
  if task_type == 'big_screen':
    df = data_processing_big_screen(df, object_type)
  elif task_type in ('poi', 'plain'):
    df = data_processing_poi_or_plain(df, object_type)
  elif task_type == 'geometry':
    df = data_processing_geometry(df, object_type)
  else:
    raise UploadTaskException('未知任务类型 {}'.format(task_type))
  return df 

def data_processing_poi_or_plain(df, object_type):
  """Update data into algo_geometry_customer or poi"""
  df = df.rename(columns={'名称': 'name',
                          '类型': 'object_type',
                          '经度': 'lng',
                          '纬度': 'lat',
                          '地址': 'address'})
  drop_columns = [
    'name', 'object_type', 'lng', 'lat', 'address', 'geometry_type']
  if isinstance(df, gpd.GeoDataFrame):
    drop_columns.append('geometry')

  extra = df.drop(drop_columns, axis=1)
  df['name'] = (df.name
                + (' ('
                   + (df
                      .groupby(['name', 'object_type'])
                      .cumcount()
                      .astype(str))
                   + ')').replace(' (0)', ''))
  df = df[drop_columns]
  df['extra'] = (extra.to_dict('records'))
  # df['user_id'] = self.current_user
  if not df.lng.isnull().any():
    # TODO: generate the geometry if lng, lat provided
    df['geometry'] = df.loc[:, ['lng', 'lat']].apply(
        lambda x: geometry_2_ewkb4326(Point(x[0], x[1])),
        axis=1)
  # df.to_pickle(object_type+'_pickle')
  return df


def data_processing_geometry(df: gpd.GeoDataFrame, object_type):
    """Update data into bd_region_special_customer"""
    df = df.rename(columns={'名称': 'name',
                            '类型': 'object_type',
                            '经度': 'lng',
                            '纬度': 'lat',
                            '地址': 'address'})
    if 'lng' not in df.columns or 'lat' not in df.columns:
      df['lng'] = df.geometry.centroid.x
      df['lat'] = df.geometry.centroid.y
    if 'address' not in df.columns:
      df['address'] = None
    else:
      df['address'] = df['address'].astype(str).str.strip()
    extra = (df
             .drop(['name', 'object_type', 'lng', 'lat', 'address', 'geometry'],
                   axis=1))
    df = df.loc[:, ['name', 'object_type', 'lng', 'lat', 'address', 'geometry']]
    for col in ['name', 'object_type']:
      df[col] = df[col].astype(str).str.strip()
    df['geometry_type'] = df.geom_type.replace({
      'Point': 'point',
      'Polygon': 'polygon',
      'MultiPolygon': 'polygon',
      'LineString': 'line',
      'MultiLineString': 'line'})
    df['name'] = (df.name
                  + (' ('
                     + (df
                        .groupby(['name', 'object_type', 'geometry_type'])
                        .cumcount()
                        .astype(str))
                     + ')').replace(' (0)', ''))
    df['geometry'] = df.geometry.apply(geometry_2_ewkb4326)
    df['extra'] = (extra.to_dict('records'))
    return df

def data_processing_big_screen(df, object_type):
  """Update data into big screen"""
  return df


def handler(event, context):
  evt = json.loads(event)
  creds = context.credentials
  # Required by OSS sdk
  auth=oss2.StsAuth(
      creds.access_key_id,
      creds.access_key_secret,
      creds.security_token)
  evt = evt['events'][0]
  bucket_name = evt['oss']['bucket']['name']
  endpoint = 'oss-' +  evt['region'] + '.aliyuncs.com'
  bucket = oss2.Bucket(auth, endpoint, bucket_name)
  path = evt['oss']['object']['key']

  dir_name = os.path.dirname(path)
  new_dir_name = dir_name.replace('upload', 'proccessed')
  base_name = os.path.basename(path)  
  extension = os.path.splitext(base_name)[-1]
  base = os.path.splitext(base_name)[0]

  target = None
  key = base.split('_', 1)[-1].strip()
  temp_dir = tempfile.TemporaryDirectory()

# get_type
  try:
    print('获取任务类型')
    task_type = load_type(extension)
  except Exception as e:
    return bucket.put_object(new_dir_name+'/stage/'+base+'_get_type_failed', str(e))
  else:
    bucket.put_object(new_dir_name+'/stage/'+base+'_get_type_succeed', 'event')


 # read_file
  try:
    print('读取文件中')
    if extension in ['.gz', '.zip', '.rar']:
      # temp_dir = tempfile.TemporaryDirectory()
      file_name = os.path.join(temp_dir.name, base_name)
      # TODO: improve this io part
      bucket.get_object_to_file(path, file_name)
      # TODO: move to pyunpack
      if extension == '.rar':
        rarfile.RarFile(file_name).extractall(temp_dir.name)
      else:
        shutil.unpack_archive(file_name, temp_dir.name)
      for i in glob(os.path.join(temp_dir.name, '**'), recursive=True):
        if i.endswith('.shp'):
          target = os.path.join(temp_dir.name, i)
      if target is None:
        raise UploadTaskException('压缩文件里没有shp文件')
    else:
      target = bucket.get_object(path)
  except Exception as e:
    logging.warning(e)
    return bucket.put_object(new_dir_name+'/stage/'+base+'_read_file_failed', str(e))
  else:
    bucket.put_object(new_dir_name+'/stage/'+base+'_read_file_succeed', 'event')

# sanity_check
  try:
    print('有效性检验中')
    df, data_type = sanity_check(target, base_name, task_type)
  except Exception as e:
    logging.warning(e)
    # bucket.put_object(new_dir_name+'/exceptionws/'+base+'.txt', str(e))
    return bucket.put_object(new_dir_name+'/stage/'+base+'_sanity_check_failed', str(e))
  else:
    bucket.put_object(new_dir_name+'/stage/'+base+'_sanity_check_succeed', 'event')

  # data_processing
  try:
    print('数据处理中')
    df = data_processing(df, task_type, key)
  except Exception as e:
    logging.warning(e)
    return bucket.put_object(new_dir_name+'/stage/'+base+'_data_processing_failed', str(e))
  else:
    pickle_tempfile = os.path.join(temp_dir.name, base+'_pickle')
    df.to_pickle(pickle_tempfile)
    bucket.put_object_from_file(new_dir_name+'/pickle/'+base+'_pickle', pickle_tempfile)
    bucket.put_object(new_dir_name+'/stage/'+base+'_data_processing_succeed', 'event')
  finally:
    if temp_dir is not None:
      temp_dir.cleanup()
