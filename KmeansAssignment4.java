import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;

import scala.Tuple2;
public class KmeansAssignment4 {
	public static void main(String args[]) {
		
        //Spark Config
        System.setProperty("hadoop.home.dir", "C:/winutils");
        SparkConf sparkConf = new SparkConf()
        .setAppName("WeatherStation")
        //Making use of 4 cores
        .setMaster("local[4]").set("spark.executor.memory", "1g");

        JavaSparkContext sparkcontext = new JavaSparkContext(sparkConf);
		
		//Hide logs for console visiblity
		sparkcontext.setLogLevel("ERROR");

		//Path to twitter2D_2 file, update accordingly to your file location.
		String path = "D:/Masters/Year 1/Sem 2/CT5150 Data Analytics/Assignment 4/twitter2D_2.txt";
		//Load up txt file into RDD.
		JavaRDD<String> tweets = sparkcontext.textFile(path);
		
		//Pair RDD for mapping our txt file data (String and Vector, Vector required for kmeans).
		JavaPairRDD<String, Vector> splitTweets = tweets.mapToPair((String e) ->
		{
			//Split on each , to break our data correctly.
			String[] splits = e.split(",");

			//parse out coordinate data stored in the JavaPairRDD. We specify our place in the split data, coords being the last 2.
			double[] coords = new double[4];
	    	for (int i = 2; i < 4; i++)
			{
	    		coords[i] = Double.parseDouble(splits[i]);
	    	}
			//Vectorize our coordinates as required for kMeansModel.
	    	return new Tuple2<>(e, Vectors.dense(coords));
		});

		//int k and int maxIterations expected for training kMeans in Apache Spark. Setting these values for future use. (4 clusters as per assignment brief)
		int k = 4;
		int maxIterations = 50;

		//our Kmeans clustering model will train with our above values. 
		KMeansModel model = KMeans.train(splitTweets.values().rdd(), k, maxIterations);
		  
		//Create a new JavaPair for our result, mapping our data to our prediciton to a tuple of the cluster and tweet.
		//splits[1] is the 2nd element of splits, which is our tweet text.
		JavaPairRDD<Integer, String> result = splitTweets.mapToPair(e ->
		{
			String[] splits = e._1.split(","); 
			return new Tuple2<>(model.predict(e._2), splits[1]);
		  });

		System.out.println("######################");
		
		//Iterate through the tuple, printing the tweet and the cluster.
		//We sort by key, ordering our clusters.
		for (Tuple2<Integer, String> out: result.sortByKey().collect())
		{
			System.out.println("Tweet " + out._2 + " is in Cluster " + out._1);
		}

		//Spam Count:

		//Duplicate JavaPairRDD, we do the same excempt map splits[0] which is our first element, telling us if it is spam or not.
		JavaPairRDD<Integer, String> resultCount = splitTweets.mapToPair(e ->
		{
			String[] splits = e._1.split(","); 
			return new Tuple2<>(model.predict(e._2), splits[0]);
		});

		System.out.println("######################");

		//Again iterate through the tuple, sorted by key.
		//This is just to confirm we are correctly counting later.
		for (Tuple2<Integer, String> out: resultCount.sortByKey().collect())
		{
			System.out.println("Spam: " + out._2 + " found in Cluster " + out._1);
		}

		System.out.println("######################");

		//Iterate through the tuple as seen above, but now we groupByKey as well, this means it'll group on
		//the cluster.
		for (Tuple2<Integer, Iterable<String>> out: resultCount.groupByKey().sortByKey().collect())
		{
			//This long filters based on the spam indicator ("1") and counts, we can then print this and repeat for next cluster.
			long count = out._2.toString().chars().filter(ch -> ch == '1').count();
			
			System.out.println("Cluster " + out._1 + " contains " + count + " spam tweets");
		}
		
	    //Shutdown our spark cluster.
		sparkcontext.stop();
		sparkcontext.close();
		  
	}
}	