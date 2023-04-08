package org.example;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.util.List;



public class Sales {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        SparkConf conf = new SparkConf().setAppName("Sales").setMaster("local[*]");
//        conf.set("spark.eventLog.enabled", "true");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        JavaRDD<String> lines = sparkContext.textFile("src/main/resources/Sales.txt");
        JavaPairRDD<String, Double> SalesMap = lines.mapToPair(x -> {
            String[] split = x.split(" ");
            return new Tuple2<>(split[0], Double.parseDouble(split[3]));
        });
        JavaPairRDD<String, Double> SalesReduce = SalesMap.reduceByKey((x, y) -> x + y);
        List<Tuple2<String, Double>> list = SalesReduce.collect();
        for (Tuple2<String, Double> tuple2 : list) {
            System.out.println(tuple2._1 + " " + tuple2._2);

        }
    }
}
