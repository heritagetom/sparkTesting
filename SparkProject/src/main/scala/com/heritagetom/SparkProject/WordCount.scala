package com.heritagetom.SparkProject

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object WordCount {
   def main(args:Array[String])={
     //start Spark Context
    val conf = new SparkConf()
      .setAppName("SparkProject")
      .setMaster("local")
    val sc = new SparkContext(conf)
    //read some text file to a test RDD
    val test = sc.textFile("big.txt")
    
    test.flatMap { line => //for each line
      line.split(" ") }//split the line in word by word- flatmap used bc we return a list
    .map { word =>// for each word
      (word,1)}//return a key/value tuple, with word as key and 1 as value
    .reduceByKey(_+_) //sum values with same key
    .saveAsTextFile("wordcount.txt") //save as a text file
    
    sc.stop() // stop the Spark Context

        
    
  
   }
}