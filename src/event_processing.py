from pyspark.sql import SparkSession
from pyspark.sql.functions import from_avro, col
from azure.eventhub import EventHubConsumerClient
import fastavro
from io import BytesIO
import matplotlib.pyplot as plt

connection_str = "<YOUR_EVENT_HUB_CONNECTION_STRING>"
eventhub_name = "<YOUR_EVENT_HUB_NAME>"

spark = SparkSession.builder.appName("RealTimeEventProcessing").getOrCreate()

# Schema for the Avro data
avro_schema = {
    "type": "record",
    "name": "Event",
    "fields": [
        {"name": "timestamp", "type": "string"},
        {"name": "value", "type": "float"}
    ]
}

def process_event(event):
    event_data = event.body_as_str()
    bytes_reader = BytesIO(event_data)
    reader = fastavro.reader(bytes_reader, avro_schema)
    records = [record for record in reader]
    df = spark.createDataFrame(records)
    df.createOrReplaceTempView("events")

    # Process data (e.g., calculate average value)
    avg_df = spark.sql("SELECT AVG(value) as avg_value FROM events")
    avg_df.show()

    # Visualization
    values = [row['avg_value'] for row in avg_df.collect()]
    plt.plot(values)
    plt.title("Average Value Over Time")
    plt.xlabel("Time")
    plt.ylabel("Average Value")
    plt.show()

client = EventHubConsumerClient.from_connection_string(connection_str, consumer_group="$Default", eventhub_name=eventhub_name)
client.receive(on_event=process_event, starting_position="-1")
