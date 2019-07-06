package com.pitt.cloudcomputing;

import java.util.Map;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class KeyCountEvaluatorWithoutCache {

	private static final String urlKey1 = "/assets/img/loading.gif";
	private static final String urlKey2 = "/assets/js/lightbox.js";
	private final static char QUOTE = '"';
	private final static char SLASH = '/';
	private final static char SPACE = ' ';
	
	private static Tuple2<String, Integer> apply(String line) {
		int quote = line.indexOf(QUOTE);
		int start = line.indexOf(SLASH, quote);
		int end = line.indexOf(SPACE, start);
		String url = line.substring(start, end).trim();
		int count = 1;
		return new Tuple2<String, Integer>(url, count);
	}
	
	private static void analysis(String[] args) {
		String file = "hdfs:///user/student/inputdata/access_log";
		if (args.length > 0) 
			file = args[0];

		SparkConf conf = new SparkConf().setAppName("Log Analysis No Cache");
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        // UrlKey1
        JavaRDD<String> lines = sc.textFile(file);
		
		JavaPairRDD<String, Integer> counts = lines.mapToPair(s -> apply(s)).
				reduceByKey((a, b) -> a + b);

		Map<String, Integer> map1 = counts.collectAsMap();
		String output1 = urlKey1 + "\t" + map1.get(urlKey1);
		
		// UrlKey2
		JavaRDD<String> newLines = sc.textFile(file);
		
		JavaPairRDD<String, Integer> newCounts = newLines.mapToPair(s -> apply(s)).
				reduceByKey((a, b) -> a + b);

		Map<String, Integer> map2 = newCounts.collectAsMap();
		String output2 = urlKey1 + "\t" + map2.get(urlKey2);
		
        System.out.println("****************OUTPUT START****************");
        System.out.println(output1);
        System.out.println(output2);
        System.out.println("****************OUTPUT END******************");
        sc.close();
    }
	
	public static void main(String[] args) {
		long start = System.currentTimeMillis();
        analysis(args);
        long end = System.currentTimeMillis();
        double time = (end - start) / 1000.0;
        System.out.println("****************RUNNING TIME START****************");
        System.out.println("Total running time without cache in seconds: " + time + "s");
        System.out.println("****************RUNNING TIME END******************");
	}
}
