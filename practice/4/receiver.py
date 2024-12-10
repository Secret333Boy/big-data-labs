from queue import Queue
import random
import time
import threading
import findspark
import matplotlib.backends.backend_webagg
findspark.init()
import matplotlib.backends
import pyspark
findspark.find()

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, from_unixtime
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType
from pyspark.ml.feature import VectorAssembler, StringIndexer, IndexToString
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

import matplotlib.pyplot as plt
import numpy as np

spark = SparkSession.builder \
  .appName("Streaming") \
  .master("local[*]") \
  .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
  .config("spark.kryoserializer.buffer.max", "1g") \
  .config("spark.executor.extraJavaOptions", "-Xss4m") \
  .config("spark.driver.extraJavaOptions", "-Xss4m") \
  .config("spark.ui.showConsoleProgress", "true") \
  .getOrCreate()
    
    
input_stream = (
  spark.readStream
  .format("socket")
  .option("host", "localhost")
  .option("port", 12341)
  .load()
)

data = input_stream.selectExpr("CAST(value AS STRING) as json_string")
parsed_data = data.selectExpr("json_string").withColumn("json_data", col("json_string").cast(StringType()))

schema = StructType([
    StructField("timestamp", LongType(), False),
    StructField("value", IntegerType(), True),
    StructField("event_type", StringType(), True),
])
final_stream_data = parsed_data.withColumn("parsed", from_json("json_string", schema))

stream_df = final_stream_data.select(
    col("parsed.timestamp").alias("timestamp"),
    col("parsed.value").alias("value"),
    col("parsed.event_type").alias("event_type")
).withColumn("timestamp", from_unixtime(col("timestamp") / 1000).cast("timestamp"))

time_window_df = stream_df.groupBy(window(col("timestamp"), "1 minute"), col("event_type")) \
  .agg(count("value").alias("count")) \
  .withColumn("window_start", col("window.start")) \
  .withColumn("window_end", col("window.end"))
  
  
  

value_map = {
    "error": (0, 5),
    "info": (30, 40),
    "warning": (20, 25),
    "critical": (6, 20)
}
def generate_data_spark(n):
    data = []
    for _ in range(n):
        event_type = random.choice(list(value_map.keys()))
        min_val, max_val = value_map[event_type]
        data.append((int(time.time() * 1000), random.randint(min_val, max_val), event_type))
    schema = StructType([
        StructField("timestamp", LongType(), False),
        StructField("value", IntegerType(), False),
        StructField("event_type", StringType(), False)
    ])
    return spark.createDataFrame(data, schema=schema)
  

  
spark_df = generate_data_spark(10000)

indexer = StringIndexer(inputCol="event_type", outputCol="label")
df_indexed = indexer.fit(spark_df).transform(spark_df)

df_indexed.show()

train_data, test_data = df_indexed.randomSplit([0.8, 0.2], seed=42)

assembler = VectorAssembler(inputCols=["value"], outputCol="features")
train_data = assembler.transform(train_data)
test_data = assembler.transform(test_data)

train_data.select("features", "label").show()

lr = LogisticRegression(featuresCol="features", labelCol="label")

lr_model = lr.fit(train_data)

predictions = lr_model.transform(test_data)

evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)

print(f"Accuracy: {accuracy}")

def predict_event_type(df):
  if not isinstance(df, pyspark.sql.DataFrame):
      raise ValueError("Input to predict_event_type must be a Spark DataFrame")
  
  df_transformed = assembler.transform(df)
  
  predictions = lr_model.transform(df_transformed)
  
  predictions.select("value", "event_type", "prediction").show()
  
  label_to_event_type = IndexToString(inputCol="prediction", outputCol="event_type_predicted", labels=indexer.labels)
  predictions_with_event_type = label_to_event_type.transform(predictions)
  predictions_with_event_type.select("value", "event_type", "event_type_predicted").show()

queue = Queue()
  
def live_plot():
  plt.ion()
  fig, ax = plt.subplots()
  plt.show()
   
  while True:
    df = queue.get()
    
    predict_event_type(df)
    
    df = df.toPandas()
    
    critical_df = df[df["event_type"] == "critical"]

    if not critical_df.empty:
      ax.clear()
      
      critical_df = critical_df.sort_values(by="window_start")
      
      x = critical_df["window_start"].astype(str)
      y = critical_df["count"]
      
      ax.bar(x, y, color="blue", alpha=0.7)
      ax.set_title("Critical Events Over Time")
      ax.set_xlabel("Window Start")
      ax.set_ylabel("Count")
      ax.set_xticklabels(x, rotation=30, ha="right")
      
      
      fig.canvas.draw()
      fig.canvas.flush_events()

plot_thread = threading.Thread(target=live_plot, daemon=True)
plot_thread.start()

def update_plot(batch_df, batch_id):
    if not batch_df.toPandas().empty:
        queue.put(batch_df)

output_stream = time_window_df.writeStream \
    .foreachBatch(update_plot) \
    .outputMode("complete") \
    .start()

output_stream.awaitTermination()

