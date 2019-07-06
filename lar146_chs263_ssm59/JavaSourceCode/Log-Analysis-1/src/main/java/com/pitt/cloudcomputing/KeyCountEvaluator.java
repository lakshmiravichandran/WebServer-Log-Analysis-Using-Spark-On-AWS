package com.pitt.cloudcomputing;

import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class KeyCountEvaluator {

	private static final String urlKey = "/assets/img/loading.gif";
	
	private static Tuple2<String, Integer> apply(String s) {
		
		String url = (s.contains(urlKey))?urlKey:null;
		int count = (url==null)?0:1;
		return new Tuple2<String, Integer>(url, count);
	}
	
	private static void analysis(String[] args) {
		String file = "hdfs:///user/student/inputdata/access_log";
		if (args.length > 0) 
			file = args[0];

		SparkConf conf = new SparkConf().setAppName("Log Analysis 1");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile(file);
		
		JavaPairRDD<String, Integer> counts = lines.mapToPair(s -> apply(s)).
				reduceByKey((a, b) -> a + b);

        List<Tuple2<String, Integer>> output = counts.collect();

        System.out.println("****************OUTPUT START****************");
        for (Tuple2<?, ?> t : output) {
        	if (null != t._1()) 
        		System.out.println(t._1() + "\t" + t._2());
        }
        System.out.println("****************OUTPUT END******************");
        sc.close();

    }
	
	public static void main(String[] args) {
		long start = System.currentTimeMillis();
        analysis(args);
        long end = System.currentTimeMillis();
        double time = (end - start) / 1000.0;
        System.out.println("****************RUNNING TIME START****************");
        System.out.println("Total running time in seconds: " + time + "s");
        System.out.println("****************RUNNING TIME END******************");
	}

}
