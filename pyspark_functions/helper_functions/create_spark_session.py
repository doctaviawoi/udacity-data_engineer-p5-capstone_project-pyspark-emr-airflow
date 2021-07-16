from pyspark.sql import SparkSession

def create_spark_session(AppName, print_sparkContext=True):
    '''
    This function starts Spark session and load config JAR packages of sas7bdat and hadoop-aws.

    Args:
        AppName (str): Spark application name.
        print_sparkContext (bool): Option to print current Spark context setting/configuration.

    Returns:
        spark (SparkSession): Spark session that is an entry point to all functionality in Spark, defaulted to True.
    '''
    spark = SparkSession \
        .builder \
        .master("local[*]")\
        .config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11")\
        .config("spark.jars.packages","org.apache.hadoop:hadoop-aws:2.7.4")\
        .appName(AppName)\
        .getOrCreate()

    #sc = spark.sparkContext
    #sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", credentials.access_key)
    #sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", credentials.secret_key)

    if print_sparkContext:
        try:
            print("SparkContext:\n")
            print(spark.sparkContext.getConf().getAll(), sep='\n')
        except:
            print("Hello! Check SparkSession.")

    return spark
