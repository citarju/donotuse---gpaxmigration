# Databricks notebook source
import pyspark
from pyspark.sql import SQLContext
import collections
# parameters:  entity
dbutils.widgets.text("entity", "TWO")
dbutils.widgets.text("storageAccount", "citmigrationstorage")
dbutils.widgets.text("dmIn", "incoming")
dbutils.widgets.text("dmMap", "maps")

entity = dbutils.widgets.get("entity")
storage_account = dbutils.widgets.get("storageAccount")
storage_key = "fs.azure.account.key.{}.blob.core.windows.net".format(storage_account)

in_folder = dbutils.widgets.get("dmIn")
map_folder = dbutils.widgets.get("dmMap")
# main files
storage_source = "wasbs://{}@{}.blob.core.windows.net".format(in_folder, storage_account)
mountpoint = "/mnt/" + entity
# map files
map_source = "wasbs://{}@{}.blob.core.windows.net".format(map_folder, storage_account)
map_mountpoint = "/mnt/map"

#print(dbutils.secrets.listscopes())
#print('scope ' + dbutils.secrets.list("storage-key-vault-scope"))

# maps per entity from source columns to dest columns
# For now, hardcode mappings but convert them later to file based or tabled based dynamic mapping
# use map to store mappings for processing

class MapService:
  def __init__(self):
    # dictionary of map dictionaries - 
    self.mappings = {}
  
  def loadMap(self, path, name, size):
    df = spark.read.csv(path, header='true', inferSchema='true')
    df.cache()
    df = df.dropna()
    self.mappings[name] = df.to_dict('records')

class TransformService:
  def __init__(self):
    self.__columnMap = collections.defaultdict()
  
  # decision - pyspark or data frame or data set
  def loadEntity(self, path, name, size, map):
    # look for the file name in configuration and load corresponding mapping into memory
    map_name = name + '_map'
    if map_name not in map.mappings:
      return None
    # main map dataframe - GP / D365 / DataEntity / Default / Rule
    main_map = map.mappings[map_name]

    #df = spark.read.format("csv").options(header='true', delimiter=',').load(path)
    source_df = spark.read.csv(path, header='true', inferSchema='true')
    source_df.cache()
    source_df = source_df.dropna()
    #source_data.take(10)
    #display(source_df)
    # map the source_df by rules defined in each row of main_map
    
    # 1. transform
    # dest_df = source_df.select('PYMTRMID').as('NAME')
    # 2. transform using sql - this should enable us to create dynamic queries
    source_df.registerTempTable(name)
    dest_df = sqlContext.sql('select PYMTRMID as NAME from ' + name)
    display(dest_df)
    return dest_df
  
  def __loadData(self, filename):
    print(filename)
  
  def generateFile(self):
    print(filename)
    # Configure blob storage account access key globally
    '''
    spark.conf.set(
      storage_key,
      sas_key)

    output_container_path = "wasbs://{}@{}.blob.core.windows.net"format(output_container_name, storage_name)
    output_blob_folder = "%s/wrangled_data_folder" % output_container_path

    # write the dataframe as a single file to blob storage
    (dataframe
     .coalesce(1)
     .write
     .mode("overwrite")
     .option("header", "true")
     .format("com.databricks.spark.csv")
     .save(output_blob_folder))
     '''
# 1. Read mapping files in the map folder
map_service = MapService()
try:
  dbutils.fs.mount(
    source = map_source,
    mount_point = map_mountpoint,
    extra_configs = {storage_key:"ZIuvDT98uFwR8gsk0DO6ZUNA2n4poxRCv1CoGbdKgPX1VlK6r7InIqvGUVwNa57RrBm8QQztFlguv9vHGLQ2xA=="}
  )
except:
  print(map_mountpoint + ' already mounted')

mapinfos = dbutils.fs.ls(mountpoint)
for path, name, size in mapinfos:
  

# 1. Read source csv files in the dpmigration folder 
# check azure vault for account credentials
# mount the container
# use key vault later - struggled with getting it to work
print(storage_source)
try:
  dbutils.fs.mount(
    source = storage_source,
    mount_point = mountpoint,
    extra_configs = {storage_key:"ZIuvDT98uFwR8gsk0DO6ZUNA2n4poxRCv1CoGbdKgPX1VlK6r7InIqvGUVwNa57RrBm8QQztFlguv9vHGLQ2xA=="}
  )
except:
  print(mountpoint + ' already mounted')

fileinfos = dbutils.fs.ls(mountpoint)
for path, name, size in fileinfos:
  print(path, name, size)
  if name == 'TWO_SY03300':
    transform_service = TransformService()
    transform_service.loadEntity(path, name, size)
  
# 2. Per entity, transform each entity based on the mapping setup data

# 3. create files in the dboutput folder



