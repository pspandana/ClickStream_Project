from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

# This is the full, unique ID of your project
project_id = "click-stream-463520"
# This is the subscription (our "viewing window") we created
subscription_id = "my-test-viewer"

# ---- Do not change anything below this line ----

# 1. Let's start the Spark engine (the robot's main power)
spark = SparkSession.builder.appName("SimpleStreamReader").getOrCreate()

# 2. Tell the robot to connect to our conveyor belt's viewing window
streaming_df = (
    spark.readStream.format("pubsub")
    .option("subscription", f"projects/{project_id}/subscriptions/{subscription_id}")
    .load()
)

# 3. The data from the belt is encoded, so we decode it into a readable string
decoded_df = streaming_df.withColumn("data", col("data").cast(StringType()))

# 4. Define the structure of our notes (this must match your CSV headers)
json_schema = StructType([
    StructField("year", StringType()),
    StructField("month", StringType()),
    StructField("day", StringType()),
    StructField("order", StringType()),
    StructField("country", StringType()),
    StructField("session ID", StringType()),
    StructField("page 1 (main category)", StringType()),
    StructField("page 2 (clothing model)", StringType()),
    StructField("colour", StringType()),
    StructField("location", StringType()),
    StructField("model photography", StringType()),
    StructField("price", StringType()),
    StructField("price 2", StringType()),
    StructField("page", StringType()),
])

# 5. Tell the robot how to parse the note using the structure we defined
parsed_df = decoded_df.withColumn("parsed_data", from_json(col("data"), json_schema)).select("parsed_data.*")

# 6. Tell the robot to print everything it reads to its own screen
query = parsed_df.writeStream.outputMode("append").format("console").start()

# 7. Wait for the job to finish (which is never, for a stream, until we stop it)
query.awaitTermination()