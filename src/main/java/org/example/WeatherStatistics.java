package org.example;
import java.io.IOException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import java.util.Comparator;
import java.util.logging.Level;
import java.util.logging.Logger;

public class WeatherStatistics {
    public static class TemperatureComparator implements Comparator<Integer> {
        public int compare(Integer a, Integer b) {
            return a.compareTo(b);
        }
    }
    public static void main(String[] args) throws IOException {
        Logger.getLogger("org").setLevel(Level.OFF);
        SparkConf conf = new SparkConf().setAppName("WeatherStatistics").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> weatherData = sc.textFile("src/main/resources/1860.csv");

        // Filter data for year 1860
        JavaRDD<String> data1860 = weatherData.filter(new Function<String, Boolean>() {
            public Boolean call(String s) {
                return s.split(",")[1].startsWith("1860");
            }
        });

        // Extract temperature data
        JavaPairRDD<String, Integer> tempData = data1860.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) {
                String[] values = s.split(",");
                return new Tuple2<>(values[2], Integer.parseInt(values[3]));
            }
        });

        // Calculate average minimum temperature
        JavaPairRDD<String, Integer> minTempData = tempData.filter(new Function<Tuple2<String,Integer>, Boolean>() {
            public Boolean call(Tuple2<String,Integer> t) {
                return t._1.equals("TMIN");
            }
        });

        double avgMinTemp = minTempData.values().reduce((a,b) -> a + b) / (double) minTempData.count();

        // Calculate average maximum temperature
        JavaPairRDD<String, Integer> maxTempData = tempData.filter(new Function<Tuple2<String,Integer>, Boolean>() {
            public Boolean call(Tuple2<String,Integer> t) {
                return t._1.equals("TMAX");
            }
        });

        double avgMaxTemp = maxTempData.values().reduce((a,b) -> a + b) / (double) maxTempData.count();

        // Calculate highest maximum temperature
        int highestMaxTemp = maxTempData.values().max(new TemperatureComparator());

        // Calculate lowest minimum temperature
        int lowestMinTemp = minTempData.values().min(new TemperatureComparator());

        System.out.println("Average minimum temperature: " + avgMinTemp);
        System.out.println("Average maximum temperature: " + avgMaxTemp);
        System.out.println("Highest maximum temperature: " + highestMaxTemp);
        System.out.println("Lowest minimum temperature: " + lowestMinTemp);
    }
}
