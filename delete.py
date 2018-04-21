import logging
import os
import json
import oss2

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
  try:
  	bucket.delete_object(path)
  except Exception as e:
  	bucket.put_object(new_dir_name+'/exceptions/'+base+'_delete_failed', e)
  else:
  	bucket.put_object(new_dir_name+'/stage/'+base+'_deleted', 'event')