package edu.ucr.cs.cs167.cho102

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.SparkConf

/**
 * @author ${user.name}
 */
object App {
  //def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)
  def main(args : Array[String]) {
    //println( "Hello World!" )
    //println("concat arguments = " + foo(args))
    val inputfile: String = args(0)

    val conf = new SparkConf
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")
    println(s"Using Spark master '${conf.get("spark.master")}'")

    val spark = SparkSession
      .builder()
      .appName("CS167_Project_App")
      .config(conf)
      .getOrCreate()

    try {
      //Load the given input file using the json format
      //DONE
      val input = spark.read.format("json")
        .option("inferSchema", "true")
        .option("header", "true")
        .load(inputfile)
      import spark.implicits._
      //input.printSchema()

      input.createOrReplaceTempView("tweets")
      //Keep only the following attributes
      // {id, text, entities.hashtags.txt, user.description, retweet_count, reply_count, and quoted_status_id}
      //DONE
      val query: String = "SELECT id, text, entities.hashtags.text AS hashtags, user.description AS user_description, retweet_count, reply_count, quoted_status_id FROM tweets;"
      val df = spark.sql(query)
     // df.printSchema()

      //Store the output in a new JSON file named tweets_clean
      //DONE
//      df.write.json("tweets_clean.json")

      //On the clean data, run a top-k SQL query to select the top 20 most frequent hashtags
      //Use the function explode to produce one list of all hashtags from the column hashtags.
      //DONE
      import org.apache.spark.sql.functions._
      val hash = df.select(explode($"hashtags"))
      //hash.show()

      //Run a count query for each hashtag.
      //DONE
      hash.createOrReplaceTempView("htags")
      val q1: String = "SELECT col, COUNT(*) AS cnt FROM htags GROUP BY col ORDER BY cnt DESC LIMIT 20;"
      val result = spark.sql(q1)
//      result.show()

      //Collect the result in an array of keywords
      val keywords = result.select("col").collect.map(f=>f.getString(0))
      println(keywords.toList)

    } finally {
      spark.stop
    }

  }

}
