package com.oudake.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

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
        JavaRDD<String> lines = sc.textFile(filePath);
        JavaRDD<String> rdd = lines.filter(s -> s.contains("s"));
        long count = rdd.count();
        System.out.println("统计字符数为: " + count);

        sc.close();
    }
}
