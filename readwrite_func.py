from pyspark.sql.functions import *
from pyspark.sql.window import Window

def read_file(desired_path,file_type):
    print('---------------------')
    print('- calling READ_FILE -')
    print('---------------------')
    print()
    '''
    UDF - read_file: 
        read_file(desired_path,file_type)
        desired_path (str)       :    define path to root folder example '/user/chadapa_met/storage_orc'
        file_type    (str)       :    'orc', 'parquet','csv'
    '''
    if file_type == 'orc':
        print('Reading ORC files at :',desired_path)
        df = spark.read.orc(desired_path)
    elif file_type == 'parquet':
        print(' Reading parquet files at :',desired_path)
        df = spark.read.format("parquet").load(desired_path)
    elif file_type == 'csv':
        print(' Reading .CSV files at :',desired_path)
        df = spark.read.option("header",True).csv(desired_path)
    else:
        print(" XXXXXXXXXXXXXXXXXXX ")
        print(" X FAILED TO READ  X ")
        print(" XXXXXXXXXXXXXXXXXXX ")

    print('----------------------')
    print('-  READ_FILE read   -')
    print('----------------------')
    print() 
    return df

def write_file(df,desired_path,file_type):
    
    print('----------------------')
    print('- calling WRITE_FILE -')
    print('----------------------')
    print()
    
    '''
    UDF - write_file(OVERWRITE as defult): 
        write_file(desired_path,file_type)
        desired_path (str)       :    define path to root folder example '/user/chadapa_met/storage_orc'
        file_type    (str)       :    'orc', 'parquet','csv'
    '''
    if file_type == 'orc':
        print(' Writing ORC files at :',desired_path)
        df.write.mode("overwrite").format("orc").save(desired_path)
        print("orc file has been written successfully")
        
    elif file_type == 'parquet':
        print(' Writing PARQUET files at :',desired_path)
        df.coalesce(120).write.mode("overwrite").format("parquet").save(desired_path)
        print("Parquet file has been written successfully")
    elif file_type == 'csv':
        print(' Writing .CSV files at :',desired_path)
        df.coalesce(1).write.format('csv').mode('overwrite')\
        .option("header", "true").option("encoding", "UTF-8").save(desired_path)
    else:
        print(" XXXXXXXXXXXXXXXXXXX ")
        print(" X FAILED TO WRITE  X ")
        print(" XXXXXXXXXXXXXXXXXXX ")
        
    print('----------------------')
    print('-  WRITE_FILE read   -')
    print('----------------------')
    print()        
    return df
