package com.mo.bigdata.spark;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class Spark {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
                .setAppName("helloworld")
                .setMaster("spark://192.168.56.4:7077");
//		SparkConf conf = new SparkConf().setMaster("node1").setAppName("Word Count");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
		JavaRDD<Integer> distData = sc.parallelize(data);
		
		
		
		JavaRDD<String> lines = sc.textFile("data.txt");
		JavaRDD<Integer> lineLengths = lines.map(s -> s.length());
		int totalLength = lineLengths.reduce((a, b) -> a + b);
	}
}
