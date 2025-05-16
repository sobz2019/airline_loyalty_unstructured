# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
# from config.config import configuration
# import logging

# logger = logging.getLogger(__name__)
"""
Main Spark streaming job for processing airline loyalty transaction data
with improved robustness and error handling
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    udf, col, from_json, to_date, lit, to_json, 
    struct, explode, array, expr, schema_of_json
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DateType, ArrayType, MapType, BooleanType
)
import sys
import os
import json
import time
import logging
import traceback

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Adjust Python path to find our modules
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from config.config import configuration
from parsers.text_parser import (
    split_transactions, extract_customer_id, extract_flight_no, extract_departure, extract_arrival,
    extract_transaction_date, extract_earned_points, extract_transaction_ref,
    extract_channel, extract_additional_data, process_transaction_text
)
from parsers.json_parser import get_json_schema

def create_common_schema():
    """Create a common schema for both text and JSON processing results"""
    return StructType([
        StructField("customer_id", StringType(), True),
        StructField("flight_no", StringType(), True),
        StructField("departure", StringType(), True),
        StructField("arrival", StringType(), True),
        StructField("transaction_date", DateType(), True),
        StructField("earned_points", IntegerType(), True),
        StructField("transaction_ref", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("additional_data", StringType(), True),
        StructField("source_type", StringType(), True)
    ])

def define_single_transaction_udf():
    """
    Define a single UDF that processes an entire transaction text.
    This reduces the number of Python UDF calls, which helps prevent crashes.
    """
    @udf(returnType=StringType())
    def process_transaction_udf(text):
        try:
            if not text:
                return None
                
            result = process_transaction_text(text)
            
            # Convert date to string for JSON serialization
            if result and 'transaction_date' in result and result['transaction_date'] is not None:
                result['transaction_date'] = result['transaction_date'].isoformat()
                
            return json.dumps(result) if result else None
        except Exception as e:
            logger.error(f"Error in process_transaction_udf: {str(e)}")
            return None
    
    return process_transaction_udf

def define_split_transactions_udf():
    """Define UDF for splitting transaction text files"""
    @udf(returnType=ArrayType(StringType()))
    def split_transactions_udf(text):
        try:
            if not text:
                return []
            return split_transactions(text)
        except Exception as e:
            logger.error(f"Error in split_transactions_udf: {str(e)}")
            return [text] if text else []
    
    return split_transactions_udf

def stream_writer(input_df: DataFrame, checkpoint_folder: str, output: str):
    """Configure and start the stream writer"""
    logger.info(f"Configuring stream writer to output to {output}")
    return (input_df
            # .repartition(num_partitions)  # Controls the number of output files
            .writeStream
            .format('parquet')
            .option('checkpointLocation', checkpoint_folder)
            .option('path', output)
            .outputMode('append')
            .trigger(processingTime='1 minute')
            .start())
    
    
def create_spark_session():
    """Initialize and configure the Spark session with AWS S3 access and Spark optimizations"""
    logger.info("Creating Spark session for LoyaltyTransactionProcessor")

    return (SparkSession.builder.appName('LoyaltyTransactionProcessor')
            .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk:1.11.469')
            .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
            .config('spark.hadoop.fs.s3a.access.key', configuration.get('AWS_ACCESS_KEY'))
            .config('spark.hadoop.fs.s3a.secret.key', configuration.get('AWS_SECRET_KEY'))
            .config('spark.hadoop.fs.s3a.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
            .config('spark.driver.memory', '4g')
            .config('spark.executor.memory', '4g')
            .config('spark.python.worker.memory', '2g')
            .config('spark.driver.memoryOverhead', '1g')
            .config('spark.executor.memoryOverhead', '1g')
            .config('spark.python.worker.reuse', 'true')
            .config('spark.python.profile', 'true')
            .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')
            .config('spark.speculation', 'false')
            .config('spark.network.timeout', '800s')
            .config('spark.executor.heartbeatInterval', '60s')
            .config('spark.driver.extraJavaOptions', '-XX:+UseG1GC')
            .config('spark.executor.extraJavaOptions', '-XX:+UseG1GC')
            .config('spark.task.maxFailures', '10')
            .config("spark.sql.streaming.schemaInference", "true")
            .config("spark.streaming.stopGracefullyOnShutdown", "true")
            .config("spark.hadoop.security.authentication", "Simple")
            .getOrCreate())

def process_text_files_efficiently(spark, text_input_dir):
    """
    Read and process text files with optimized UDF usage.
    Instead of using multiple UDFs, we use a single UDF that processes the entire transaction.
    """
    logger.info(f"Setting up text file processing from {text_input_dir}")
    
    try:
        # Define UDFs
        split_transactions_udf = define_split_transactions_udf()
        process_transaction_udf = define_single_transaction_udf()
        
        # Read text files
        text_df = (spark.readStream
                  .format('text')
                  .option('wholetext', 'true')
                  .load(text_input_dir))
        
        # First, split the file into individual transactions
        transactions_df = text_df.withColumn('transactions', split_transactions_udf('value'))
        
        # Explode the array of transactions to process each one
        exploded_df = transactions_df.select(explode('transactions').alias('transaction'))
        
        # Process each transaction using a single UDF that handles all extraction
        processed_json_df = exploded_df.withColumn('transaction_json', process_transaction_udf('transaction'))
        
        # Filter out null results
        filtered_df = processed_json_df.filter(col('transaction_json').isNotNull())
        
        # Define the schema for the JSON data
        common_schema = create_common_schema()
        
        # Parse the JSON result and extract fields
        parsed_df = filtered_df.select(from_json('transaction_json', common_schema).alias('parsed_data'))
        
        # Extract fields from the parsed data
        final_df = parsed_df.select(
            col('parsed_data.customer_id'),
            col('parsed_data.flight_no'),
            col('parsed_data.departure'),
            col('parsed_data.arrival'),
            col('parsed_data.transaction_date'),
            col('parsed_data.earned_points'),
            col('parsed_data.transaction_ref'),
            col('parsed_data.channel'),
            col('parsed_data.additional_data'),
            col('parsed_data.source_type')
        )
        
        return final_df
    except Exception as e:
        logger.error(f"Error in process_text_files_efficiently: {str(e)}")
        logger.error(traceback.format_exc())
        
        # Return an empty DataFrame with the correct schema as fallback
        common_schema = create_common_schema()
        return spark.createDataFrame([], common_schema)

def process_json_files(spark, json_input_dir):
    """Read and process JSON files with error handling"""
    logger.info(f"Setting up JSON file processing from {json_input_dir}")
    
    try:
        # Get JSON schema
        json_schema = get_json_schema()
        
        # Read JSON files
        json_df = (spark.readStream
                   .format("json")
                   .schema(json_schema)
                   .option("multiLine", "true")
                   .option("mode", "PERMISSIVE")  # Be permissive with JSON parsing errors
                   .option("columnNameOfCorruptRecord", "_corrupt_record")
                   .load(json_input_dir))
        
        # Process JSON files
        processed_json_df = (json_df
                        .select(
                            col('customer_id'),
                            col('flight_details.flight_no').alias('flight_no'),
                            col('flight_details.departure').alias('departure'),
                            col('flight_details.arrival').alias('arrival'),
                            to_date(col('flight_details.date')).alias('transaction_date'),
                            col('transaction_info.earned_points').alias('earned_points'),
                            col('transaction_info.transaction_ref').alias('transaction_ref'),
                            col('transaction_info.channel').alias('channel'),
                            # Combine additional_info and promo_details into a JSON string
                            to_json(struct('additional_info', 'promo_details')).alias('additional_data')
                        )
                        .withColumn('source_type', lit('json')))
        
        return processed_json_df
    except Exception as e:
        logger.error(f"Error in process_json_files: {str(e)}")
        logger.error(traceback.format_exc())
        
        # Return an empty DataFrame with the correct schema as fallback
        common_schema = create_common_schema()
        return spark.createDataFrame([], common_schema)

def monitor_streaming_query(query, app_name):
    """Monitor the streaming query and log its status with error handling"""
    while query.isActive:
        try:
            logger.info(f"{app_name} - Active: {query.isActive}, Data available: {query.status.get('isDataAvailable', False)}")
            if 'sources' in query.status and query.status['sources']:
                logger.info(f"Latest offsets: {query.status['sources'][0].get('startOffset')} to {query.status['sources'][0].get('endOffset')}")
            else:
                logger.info("No offset information available yet (possibly no data processed).")
        except Exception as e:
            logger.warning(f"Error accessing streaming query status: {e}")
        
        time.sleep(60)  # Check status every minute
        
        
def main():
    """Main entry point for the Spark streaming job with improved error handling"""
    try:
        # Initialize Spark session
        spark = create_spark_session()
        logger.info("Spark session initialized successfully")

        # Define input and output paths
        text_input_dir = f"s3a://{configuration.get('S3_BUCKET')}/raw/text/"
        json_input_dir = f"s3a://{configuration.get('S3_BUCKET')}/raw/json/"

        logger.info(f"Input paths - Text: {text_input_dir}, JSON: {json_input_dir}")
        
        # Process text files using optimized approach
        processed_text_df = process_text_files_efficiently(spark, text_input_dir)
        
        # Process JSON files
        processed_json_df = process_json_files(spark, json_input_dir)
        
        # Union the dataframes
        union_df = processed_text_df.unionByName(
            processed_json_df, 
            allowMissingColumns=True
        )
        logger.info("Dataframes unioned successfully")
        

        # Configure output paths
        checkpoint_path = f"s3a://{configuration.get('S3_BUCKET')}/checkpoints/"
        output_path = f"s3a://{configuration.get('S3_BUCKET')}/processed/"

        logger.info(f"Output path: {output_path}, Checkpoint path: {checkpoint_path}")
        
        # Start the streaming job
        query = stream_writer(union_df, checkpoint_path, output_path)
        logger.info("Streaming job started successfully")
        
        
        # ADD THIS CODE: Create a second query that outputs to console
        console_query = union_df.writeStream \
            .format("console") \
            .outputMode("append") \
            .option("truncate", "false") \
            .option("numRows", 5) \
            .start()
        logger.info("Console debug stream started")
    
    
        # Monitor the query
        monitor_streaming_query(query, "LoyaltyTransactionProcessor")
        
        # Await termination
        # query.awaitTermination()
        spark.streams.awaitAnyTermination()

    except Exception as e:
        logger.error(f"Error in main streaming job: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    main()