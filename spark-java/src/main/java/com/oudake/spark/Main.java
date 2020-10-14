package com.oudake.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class Main {

    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("缺少路径参数");
            return;
        }
        String filePath = args[0];
        SparkConf conf = new SparkConf()
                .setAppName("")
                .setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> fileRDD = sc.textFile(filePath);
        JavaRDD<String> lines = sc.parallelize(fileRDD.collect());
        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
        JavaPairRDD<String, Integer> pairs = words.mapToPair(word -> new Tuple2<>(word, 1));
        JavaPairRDD<String, Integer> wordCounts = pairs.reduceByKey(Integer::sum);

        System.out.println("统计字符结果为:");
        wordCounts.foreach(pair -> System.out.println(pair._1 + " : " + pair._2));

        sc.close();
    }
}
