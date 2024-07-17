import logging
from datetime import datetime
from cassandra.auth import PlainTextAuthenticator
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define functions

# Create Cassandra keyspace and table
def create_keyspace(session):
    """Create Cassandra keyspace if it doesn't exist."""
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_stream 
        WITH REPLICATION = 
        { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
    )

def create_table(session):
    """Create Cassandra table if it doesn't exist."""
    session.execute("""
        CREATE TABLE IF NOT EXISTS spark_stream.users_data (
            user_id UUID PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            gender TEXT,
            address TEXT,
            post_code TEXT,
            email TEXT,
            username TEXT,
            dob TIMESTAMP,
            registered_date TIMESTAMP,
            phone TEXT,
            picture TEXT
        )
    """)

# Create Spark session
def create_spark_connection(retries=5, delay=5, cassandra_host="cassandra"):
    """
    Create a Spark session with retries and configurable delay.
    
    Parameters:
    retries (int): Number of retries for creating Spark session.
    delay (int): Delay in seconds between retries.
    cassandra_host (str): Hostname or IP address of the Cassandra node.
    
    Returns:
    SparkSession: A SparkSession object if successful, None otherwise.
    """
    for attempt in range(retries):
        try:
            logging.info(f"Attempt {attempt + 1} to create Spark session")
            spark = SparkSession.builder \
                .appName("Cassandra Spark Streaming") \
                .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.13:3.5.1,"
                                               "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1") \
                .config("spark.cassandra.connection.host", cassandra_host) \
                .getOrCreate()
            spark.sparkContext.setLogLevel("ERROR")
            logging.info("Spark session created successfully")
            return spark
        except Exception as e:
            logging.error(f"Failed to create Spark session: {e}")
            if attempt < retries - 1:
                logging.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)
    logging.error("Exhausted all retries. Spark session could not be created.")
    return None

# Create Cassandra session
def create_cassandra_connection():
    """Create and return a Cassandra session."""
    try:
        cluster = Cluster(['cassandra'], auth_provider=PlainTextAuthenticator('cassandra', 'cassandra'))
        session = cluster.connect()
        logging.info("Cassandra session created successfully")
        return session
    except Exception as e:
        logging.error(f"Failed to connect to Cassandra: {e}")
        return None

# Connect to Kafka 
def connect_kafka(session):
    """Connect to Kafka and return a DataFrame with the Kafka data."""
    try:
        kafka_df = session.readStream \
                    .format("kafka") \
                    .option("kafka.bootstrap.servers", "broker:29092") \
                    .option("subscribe", "users_created") \
                    .option("startingOffsets", "earliest") \
                    .load()
        value_df = kafka_df.selectExpr("CAST(value AS STRING)")
        logging.info("Connected to Kafka successfully")
        return value_df
    except Exception as e:
        logging.error(f"Failed to connect to Kafka: {e}")
        return None

# Parse Kafka data
def parsed_kafka_data(kafka_df):
    """Parse the Kafka data and return a DataFrame."""
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("address", StringType(), True),
        StructField("post_code", StringType(), True),
        StructField("email", StringType(), True),
        StructField("username", StringType(), True),
        StructField("dob", TimestampType(), True),
        StructField("registered_date", TimestampType(), True),
        StructField("phone", StringType(), True),
        StructField("picture", StringType(), True)
    ])

    try:
        parsed_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("data")) \
            .select("data.*")
        logging.info("Parsed Kafka data successfully")
        return parsed_df
    except Exception as e:
        logging.error(f"Failed to parse Kafka data: {e}")
        return None

if __name__ == '__main__':
    # Create Spark session
    spark_session = create_spark_connection()

    if spark_session:
        # Connect to Kafka and parse the data
        spark_df = connect_kafka(spark_session)
        if spark_df:
            parsed_df = parsed_kafka_data(spark_df)
            if parsed_df:
                # Create Cassandra session and set up keyspace and table
                cassandra_session = create_cassandra_connection()
                if cassandra_session:
                    create_keyspace(cassandra_session)
                    create_table(cassandra_session)
                    logging.info("Streaming is being started...")

                    # Write stream to Cassandra
                    streaming_query = (parsed_df.writeStream.format("org.apache.spark.sql.cassandra")
                                       .option('checkpointLocation', '/tmp/checkpoint')
                                       .option('keyspace', 'spark_stream')
                                       .option('table', 'users_data')
                                       .start())

                    streaming_query.awaitTermination()
