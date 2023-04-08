package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
public class Main {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        SparkConf conf = new SparkConf().setAppName("Application 1").setMaster("local[*]");;
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        JavaRDD<String> RDD1 = sparkContext.parallelize(Arrays.asList("Zin", "Edd", "Youssef", "Ben"));
        JavaRDD<String> RDD2 = RDD1.flatMap(x -> Arrays.asList(((String)x).split(" ")).iterator());
        JavaRDD<String> RDD3 = RDD2.filter(x-> {
            return x.contains("Z");
        });
        JavaRDD<String> RDD4 = RDD2.filter(x-> {
            return x.contains("Y");
        });
        JavaRDD<String> RDD5 = RDD2.filter(x-> {
            return x.contains("E");
        });
        JavaRDD<String> RDD6 = sparkContext.union(RDD3, RDD3);
        JavaPairRDD<String,Integer> RDD71 = RDD5.mapToPair(x->new Tuple2<>(x,1));
        JavaPairRDD<String,Integer> RDD81 = RDD6.mapToPair(x->new Tuple2<>(x,1));
        JavaPairRDD<String,Integer> RDD7 = RDD71.reduceByKey((x,y)->x+y);
        JavaPairRDD<String,Integer> RDD8 = RDD81.reduceByKey((x,y)->x+y);
        JavaRDD<String> RDD9 = sparkContext.union(RDD7.keys(), RDD8.keys());
        JavaRDD<String> RDD10 = RDD9.sortBy(x->x, true, 1);
        List<String> result = RDD10.collect();
        for (String s : result) {
            System.out.println(s);
        }
    }
}
