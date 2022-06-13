from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import Window
from pyspark.sql.functions import when, col ,row_number ,max as max1 , min as min1 ,count


if __name__ == '__main__':
    spark=SparkSession.builder\
           .appName("Airline Data Management") \
           .master("local[*]")\
           .getOrCreate()
    print(spark)

    # rdu=ReadDataUtil()

    airlineschema = StructType([
        StructField("airline_id",IntegerType()),
        StructField("name",StringType()),
        StructField("alias",StringType()),
        StructField("iata code",StringType()),
        StructField("icoa code",StringType()),
        StructField("Callsign",StringType()),
        StructField("country",StringType()),
        StructField("Active",StringType())
    ])

    # airline_df=rdu.readCsv(spark=spark,path=r"C:\Users\Sagar\Desktop\csv\Airport data\airline.csv",schema=airlineschema)
    # # df.show()
    # airlineschema.printSchema()

    #========================================================

    ## Reading airline csv file

    airline_df=spark.read.csv(r"C:\Users\Sagar\Desktop\csv\Airport data\airline.csv",schema=airlineschema)
    # airline_df.printSchema()
    # airline_df.show()

    airline_df.createOrReplaceTempView('airline_df')


    # windowspec=Window.orderBy("airline_id")
    # airline_df=airline_df.withColumn("rownum",row_number().over(windowspec))\
    #     .filter(col("rownum") != 1).drop("rownum")

    # airline_df=airline_df.withColumn("alias",when(col("alias") == r"\N",None).otherwise(col("alias")))

    # airline_df=airline_df.na.fill('(unknown)',subset=['alias','iata code','icoa code','Callsign','country'])

    # airline_df.show()

    ##  In any of input file if you are getting \N or null in the column and that column is of string type then put default values as "(unkwown)" and if column is of type integer then put -1

    # =================================================================================

    ## Reading airport csv file

    airportschema=StructType([StructField("airport_id",IntegerType()),
                              StructField("name", StringType()),
                              StructField("city", StringType()),
                              StructField("country", StringType()),
                              StructField("iata", StringType()),
                              StructField("icao", StringType()),
                              StructField("lattitude", DecimalType(15,10)),
                              StructField("longitude", DecimalType(15,10)),
                              StructField("altitude", IntegerType()),
                              StructField("timezone", IntegerType()),
                              StructField("dst", StringType()),
                              StructField("tz", StringType()),
                              StructField("type", StringType()),
                              StructField("source", StringType())
                              ])

    airport_df=spark.read.csv(r"C:\Users\Sagar\Desktop\csv\Airport data\airport.csv",schema=airportschema)
    # airport_df.show()

    airport_df.createOrReplaceTempView('airport_df')

    # ------
    # airport_df=airport_df.withColumn("rownum",row_number().over(Window.orderBy("airline_id")))

    # airport_df=airport_df.withColumn('iata',when(col('iata') == r'\N', None).otherwise(col('iata')))
    #
    # airport_df=airport_df.na.fill('(unkwown)',subset= ['iata'])
    # airport_df=airport_df.na.fill(-1, subset= ['timezone'])
    # airport_df.show()

    #=================================================================================

    ## Reading Plane Schema

    plane_schema=StructType([StructField('name',StringType()),
                             StructField('iata_code',StringType()),
                             StructField('icao_code',StringType())
                             ])

    plane_df=spark.read.csv(r"C:\Users\Sagar\Desktop\csv\Airport data\plane.csv",schema=plane_schema)
    plane_df.createOrReplaceTempView('plane_df')
    # plane_df.show()
    #=================================================================================

    ## Reading Route File

    routes_schema=StructType([StructField('airline',StringType()),
                              StructField('airline_id', IntegerType()),
                              StructField('src_airport', StringType()),
                              StructField('src_airport_id', IntegerType()),
                              StructField('dest_airport', StringType()),
                              StructField('dest_airport_id', IntegerType()),
                              StructField('codeshare', StringType()),
                              StructField('stops', IntegerType()),
                              StructField('equipment', StringType())
                              ])

    routes_df=spark.read.parquet(r"C:\Users\Sagar\Desktop\csv\Airport data\routes.snappy.parquet",
                                 schema=routes_schema)
    routes_df.createOrReplaceTempView(" routes_df")
    # routes_df.show()

    #===================================================================================

    ## Q1 Find the country which having both airport and airline

    # airport_df.select('country').intersect(airline_df.select('country')).show()
    # print("======================================================================")
    # spark.sql(" Select country from airport_df "
    #           "intersect "
    #           " Select country from airline_df").show()

    #=====================================================================================

    ## Q2 Get the airlines details like name ,id which is taken takoff more then 3 times from same airport

    # spark.sql(" Select name , airline_id from airline_df ")

    ## Using Dataframe
    # airline_df.join(routes_df, 'airline_id').select(routes_df.airline_id,airline_df.name,'src_airport') \
    #           .groupBy(routes_df.airline_id,airline_df.name,'src_airport') \
    #         .count().filter(col('count') > 3).show()

    ## Using Spark Sql
    # spark.sql(" Select distinct a1.name airline_name , a1.airline_id, src_airport ,count(src_airport) takeoffs "
    #           " from routes_df r "
    #           " join airline_df a1 "
    #           " on r.airline_id = a1.airline_id "
    #           " group by a1.name,a1.airline_id, src_airport "
    #           " having count(src_airport) > 3").show()

    #===================================================================================

    ## Q3 Get airport details which has minimum number of takeoffs and landing

    takeoffs_df=spark.sql(" Select src_airport, count(src_airport) takeoffs "
                          " from routes_df "
                          " GROUP BY src_airport")

    landings_df=spark.sql(" Select dest_airport ,count(dest_airport) landings "
                          " from routes_df "
                          " GROUP BY dest_airport ")

    joinned_df=takeoffs_df.join(landings_df, on=takeoffs_df.src_airport == landings_df.dest_airport, how='full') \
                          .na.fill(0,['takeoffs','landings']).na.fill('NA',subset=['src_airport']) \
                          .withColumn('airport', when(col('src_airport') == "NA", col('dest_airport'))
                                      .otherwise(col('src_airport'))) \
                          .select(col('airport'),(col('takeoffs')+col('landings')).alias('total'))

    min_total=joinned_df.select(min1(col('total'))).collect()[0][0]
    airport_with_min=joinned_df.filter(col('total') ==min_total).select('airport').rdd.flatMap(lambda x: x) \
                               .collect()
    print(airport_with_min)

    print("=========================================================================")

    print(airport_df.filter(airport_df.iata.isin(airport_with_min) | airport_df.icao.isin(airport_with_min)).count())

    #=========================================================================================

    ## Q4 Get the airport  details which is having maximum number of takeoffs and landings

    takeoffs_df = spark.sql(" select src_airport , count(src_airport) takeoffs "
                          " from routes_df "
                          " group by src_airport")

    landings_df = spark.sql(" select dest_airport ,count(dest_airport) landings "
                            " from routes_df "
                            "group by dest_airport")

    joinned_df=takeoffs_df.join(landings_df,on=takeoffs_df.src_airport == landings_df.dest_airport, how="full") \
                          .na.fill(0,['takeoffs','landings']).na.fill('NA',subset=['src_airport']) \
                          .withColumn('airport',when(col('src_airport') == 'NA' , col('dest_airport'))
                                      .otherwise(col('src_airport'))) \
                          .select(col('airport'),(col('takeoffs') + col('landings')).alias('total'))

    # max_total = joinned_df.select(max1(col('total'))).collect()[0][0]
    # airport_with_max=joinned_df.filter(col('total') == max_total).select('airport')\
    #                                                                 .rdd.flatMap(lambda x: x).collect()
    # airport_df.filter(airport_df.iata.isin(airport_with_max)).show()
    # print(max_total)

    #==========================================================================================

    ## Q5 Get the airline details , which is having direct flights , details like airline id, name ,
    ## source airport name and destination airport name

    # r=routes_df.alias('r')
    # a1=airline_df.alias('a1')
    # ap1=airport_df.withColumnRenamed('name','source_airport').select('*')
    # ap2=airport_df.withColumnRenamed('name','destination_airport').select('*')
    #
    # r.join(a1,'airline_id','left_outer').join(ap1,r.src_airport_id == ap1.airport_id,'left_outer') \
    #      .join(ap2,r.dest_airport_id == ap2.airport_id, 'left_outer').filter(col('stops') == 0) \
    #      .select(a1.airline_id,a1.name.alias('airline_name'), ap1.source_airport, ap2.destination_airport).show()

    # ## Without nulls in airline id and name
    # spark.sql(" select a1.airline_id, a1.name airline_name, ap1.name source_airport, ap2.name  dest_airport "
    #           " from routes_df r "
    #           " join airline_df a1 on r.airline_id = a1.airline_id "
    #           " left join airport_df ap1 on r.src_airport_id = ap1.airport_id "
    #           "left join airport_df ap2 on r.dest_airport_id = ap2.airport_id "
    #           " where stops = 0 order by a1.airline_id").show(n=50)

    #========================================================================================

    ## Q6 Find the most popular routes i.e route with max trips

    #=====================================================================================

    ## Q7 And find whch airlines are operational on that routes

    # Required details (source_airport_name|airport_id|code|dest_airport_name|code|airport_id|total_trips on this route)

    routes_max_trip=spark.sql(" select ap1.name source_airport,y.src_airport_id, y.src_airport_id, y.src_airport, "
                              " ap2.name destination_airport, y.dest_airport_id, y.dest_airport, trips from "
                              " (select x.*, rank() over(order by trips desc) as rank1 from "
                              " (select src_airport_id, src_airport, dest_airport_id, dest_airport, count(*) trips "
                              " from routes_df "
                              " group by src_airport_id, src_airport, dest_airport_id, dest_airport)x "
                              " )y "
                              " join airport_df ap1 on y.src_airport_id = ap1.airport_id "
                              " join airport_df ap2 on y.dest_airport_id = ap2.airport_id "
                              " where rank1 = 1 " )

    routes_max_trip.show()
    routes_max_trip.createOrReplaceTempView('max_trips')

    spark.sql(" select a1.name airline_name, airline, a1.airline_id, r.src_airport, r.dest_airport "
              " from routes_df r join max_trips m "
              " on r.src_airport = m.src_airport and r.dest_airport = m.dest_airport "
              " join airline_df a1 on r.airline_id = a1.airline_id ").show()