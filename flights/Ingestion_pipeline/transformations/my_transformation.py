from pyspark import pipelines as dp
from pyspark.sql.types import *
from pyspark.sql.functions import *

## Bookings

@dp.table
def stage_bookings():
    df = spark.readStream.table("flights.bronze.bookings")
    return df

@dp.view

def bookings_view():
    df = spark.readStream.table("stage_bookings")
    df = df.withColumn("amount",col("amount").cast(DoubleType())).withColumn("modifiedDate",current_timestamp()).withColumn("booking_date",to_date(col("booking_date"))).drop("_rescued_data")
    return df

rules = {
    "rule_one":"booking_id IS NOT NULL",
    "rule_two":"passenger_id IS NOT NULL"
}

@dp.table

@dp.expect_all_or_drop(rules)
def dim_bookings():
    df = spark.readStream.table("bookings_view")
    return df


##Flights

@dp.table
def dim_flights():
    df = spark.readStream.table("flights.silver.dim_flights")
    return df

@dp.view
def flights_view():
    df = spark.readStream.table("dim_flights")
    df = df.withColumn("modifiedDate",current_timestamp())
    return df


dp.create_streaming_table("flights")

dp.create_auto_cdc_flow(
    target = "flights",
    source="flights_view",
    keys = ["flight_id"],
    sequence_by= col("modifiedDate"),
    stored_as_scd_type=1
)

##Flights

@dp.table
def dim_passengers():
    df = spark.readStream.table("flights.silver.dim_passengers")
    return df

@dp.view
def passengers_view():
    df = spark.readStream.table("dim_passengers")
    df = df.withColumn("modifiedDate",current_timestamp())
    return df


dp.create_streaming_table("passengers")

dp.create_auto_cdc_flow(
    target = "passengers",
    source="passengers_view",
    keys = ["passenger_id"],
    sequence_by= col("modifiedDate"),
    stored_as_scd_type=1
)


##airports


@dp.table
def dim_airports():
    df = spark.readStream.table("flights.silver.dim_airports")
    return df

@dp.view
def airports_view():
    df = spark.readStream.table("dim_airports")
    df = df.withColumn("modifiedDate",current_timestamp())
    return df


dp.create_streaming_table("airports")

dp.create_auto_cdc_flow(
    target = "airports",
    source="airports_view",
    keys = ["airport_id"],
    sequence_by= col("modifiedDate"),
    stored_as_scd_type=1
)

@dp.table

def gold_business():
    df = dp.readStream("dim_bookings").join(dp.readStream("flights"),["flight_id"]).join(dp.readStream("passengers"),["passenger_id"]).join(dp.readStream("airports"),["airport_id"]).drop("create_date","update_date","modifiedDate")
    return df



