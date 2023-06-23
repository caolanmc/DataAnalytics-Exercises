import java.util.Arrays;

import org.apache.spark.*;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;

import scala.Tuple2;


public class assignment5 extends Twotter
{
    public static void main(String[] args)
    {
        //Question 1
        //Initial set up of spark instance and streaming context
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("Twotter");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(2));

        //Comment the below line if you wish to bring back spark logs.
        jssc.sparkContext().setLogLevel("OFF");

        //Twotter runs locally, so we call localhost and the same port as per Twotter.java (9999)
        JavaReceiverInputDStream<String> tweets = jssc.socketTextStream("localhost", 9999);

        //Set our window times as per assignment brief (Sliding window 6seconds, sliding interval length 2seconds).
        JavaDStream<String> windowStream = tweets.window(Durations.seconds(6), Durations.seconds(2));
        //Map any streamed data element
        JavaDStream<String> tweetStream = windowStream.map(e -> e);             
        //Print above mentioned element.
        tweetStream.print();


        //Question 2

        //Create a new DStream of our previously mapped tweets, flatMap the RDD, splitting them on the white space.
        JavaDStream<String> words = tweetStream//.window(Durations.seconds(6), Durations.seconds(2))
        .flatMap(tweet -> Arrays.asList(tweet.split(" ")).iterator());

        //A new DStream is created for given tasks, in the below we look for the "@".
        //We do this through a filter and startsWith(), then using map to pair to allow for future counting.
        JavaPairDStream<String, Integer> usernameAt = words
        .filter( word -> word.startsWith("@"))
        .mapToPair(e -> new Tuple2<String, Integer>(e, 1));

        //We use the below reduceByKey, which effectively counts our given username (@), then prints each element.
        usernameAt.reduceByKey((a, b) -> a + b)
        .foreachRDD(sort -> sort
        .foreach(e -> System.out.println(e)));

        //Below is the exact same as above, except we traide out for a "#" symbol, effectively counting and printing hashtags.
        JavaPairDStream<String, Integer> hashtag = words
        .filter( word -> word.startsWith("#"))
        .mapToPair(e -> new Tuple2<String, Integer>(e, 1));

        hashtag.reduceByKey((a, b) -> a + b)
        .foreachRDD(sort -> sort
        .foreach(e -> System.out.println(e)));

        //usernameAt.print();
        //hashtag.print();


        //Question 3

        //getting the most frequent hashtag is effectively the same method as the previous question, we actually call the hashtag dStream as to not repeat code,
        //where we reduceByKey to get the count, then we sort and take the top 1, giving us the hashtag with the highest count.
        hashtag.reduceByKey((a, b) -> a + b)
        .foreachRDD(sort -> sort
        .mapToPair(e -> e.swap())
        .sortByKey(false)
        .take(1)
        .forEach(e -> System.out.println("Most frequent hashtag: " + e)));



        //Question 4

        //In progress

        //To Do - Stream of total usernamer/hashtag and stream of count
        //divide total/count
        //Wrap and print


        //Using original JavaDStream "words", to calculate total word count using .count()
        words.foreachRDD(e -> {
            long wordCount = e.count();
            System.out.println("Total words: " + wordCount);
        });

        //Need to divide given hashtag or username by the above total word count. 
        //Unsure of how to proceed, potentially wrap the two streams and combine.
        usernameAt.reduceByKey((a, b) -> a + b)
        .foreachRDD(sort -> sort
        .foreach(e -> System.out.println(e)));

        hashtag.reduceByKey((a, b) -> a + b)
        .foreachRDD(sort -> sort
        .foreach(e -> System.out.println(e)));



        //Start the stream.
        jssc.start();
        try
        {
            jssc.awaitTermination();
        } catch (InterruptedException e)
        {
            e.printStackTrace();
        }
        jssc.stop();











    }
}