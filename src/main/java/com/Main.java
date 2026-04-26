package com;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class Main {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("city_analysis")
                .master("local[*]")
                .getOrCreate();

        String file_path = "src/main/resources/data/data.csv";

        StructType schema = new StructType()
                .add("rental_id", DataTypes.StringType,false)
                .add("user_id", DataTypes.StringType, false)
                .add("age", DataTypes.IntegerType, false)
                .add("gender", DataTypes.StringType, false)
                .add("start_time", DataTypes.TimestampType, false)
                .add("end_time", DataTypes.TimestampType, false)
                .add("start_station", DataTypes.StringType, false)
                .add("end_station", DataTypes.StringType, false)
                .add("duration_minutes", DataTypes.IntegerType, false)
                .add("price", DataTypes.DoubleType, false);

        Dataset<Row> df = spark.read()
                .format("csv")
                .option("header","true")
                .schema(schema)
                .load(file_path);

        //Display the schema
        df.printSchema();

        //Show the 5 first rows
        df.show(5);

        //Number of rentals
        System.out.println(df.count());

        //Create temporary view
        df.createOrReplaceTempView("bike_rentals_view");

        //Basic SQL queries

        spark.sql("""
            SELECT * FROM bike_rentals_view
            WHERE duration_minutes > 30
        """).show();

        spark.sql("""
            SELECT * FROM bike_rentals_view
            WHERE start_station == 'River Station'
        """).show();

        spark.sql("""
            SELECT SUM(price) AS total_price
            FROM bike_rentals_view
        """).show();


        //Aggregations
        spark.sql("""
            SELECT start_station, COUNT(rental_id)
            FROM bike_rentals_view
            GROUP BY start_station
        """).show();

        spark.sql("""
            SELECT start_station, AVG(duration_minutes)
            FROM bike_rentals_view
            GROUP BY start_station
        """).show();

        spark.sql("""
            SELECT start_station, COUNT(rental_id) AS total_rentals
            FROM bike_rentals_view
            GROUP BY start_station
            ORDER BY total_rentals DESC
            LIMIT 1
        """).show();


        spark.sql("""
            SELECT HOUR(start_time)
            FROM bike_rentals_view
        """).show();

        spark.sql("""
            SELECT MINUTE(start_time)
            FROM bike_rentals_view
        """).show();

        spark.sql("""
            SELECT HOUR(start_time) AS hour,
                   COUNT(rental_id) AS total_rentals
            FROM bike_rentals_view
            GROUP BY HOUR(start_time)
            ORDER BY total_rentals DESC
        """).show();

        spark.sql("""
            SELECT start_station,
                   COUNT(rental_id) AS total_rentals
            FROM bike_rentals_view
            WHERE HOUR(start_time) BETWEEN 7 AND 12
            GROUP BY start_station
            ORDER BY total_rentals DESC
            LIMIT 1
        """).show();


        spark.sql("""
            SELECT AVG(age)
            FROM bike_rentals_view
        """).show();


        spark.sql("""
            SELECT gender, COUNT(user_id)
            FROM bike_rentals_view
            GROUP BY gender
        """).show();

        spark.sql("""
            SELECT 
                CASE 
                    WHEN age BETWEEN 18 AND 30 THEN '18-20'
                    WHEN age BETWEEN 31 AND 40 THEN '31-40'
                    WHEN age BETWEEN 41 AND 50 THEN '41-50'
                    WHEN age >= 51 THEN '51+'
                END AS age_group,
                COUNT(rental_id) AS total_rentals
            FROM bike_rentals_view
            GROUP BY 
                CASE 
                    WHEN age BETWEEN 18 AND 30 THEN '18-20'
                    WHEN age BETWEEN 31 AND 40 THEN '31-40'
                    WHEN age BETWEEN 41 AND 50 THEN '41-50'
                    WHEN age >= 51 THEN '51+'
                END
            ORDER BY total_rentals DESC 
            
        """).show();

        spark.stop();

    }
}