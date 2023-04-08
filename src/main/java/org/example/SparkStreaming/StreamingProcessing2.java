package org.example.SparkStreaming;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

public class StreamingProcessing2 {
    public static void main(String[] args) throws InterruptedException {
        Logger.getLogger("org").setLevel(Level.OFF);
        SparkConf conf = new SparkConf().setAppName("Streaming Processing").setMaster("local[*]");
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Duration.apply(20000));
        JavaDStream<String> lines = streamingContext.textFileStream("./names");
        JavaDStream<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        JavaPairDStream<String, Integer> pairs = words.mapToPair(word -> new Tuple2<>(word, 1));
        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey((x, y) -> x + y);
        wordCounts.print();
        streamingContext.start();
        streamingContext.awaitTermination();
    }
}
