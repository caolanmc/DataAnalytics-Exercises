
// $example on$
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
// $example off$
import org.apache.spark.sql.SparkSession;


/**
 * An example demonstrating k-means clustering.
 * Run with
 * <pre>
 * bin/run-example ml.JavaKMeansExample
 * </pre>
 */
public class JavaKMeansExample {

  public static void main(String[] args) {
    // Create a SparkSession.
        //Spark Config
        System.setProperty("hadoop.home.dir", "C:/winutils");
        SparkConf sparkConf = new SparkConf()
        .setAppName("WeatherStation")
        //Making use of 4 cores
        .setMaster("local[4]").set("spark.executor.memory", "1g");

        SparkSession spark = SparkSession
        .builder()
        .appName("JavaKMeansExample")
        .getOrCreate();

        JavaSparkContext sparkcontext = new JavaSparkContext(sparkConf);
		
		    //Hide logs for console visiblity
			sparkcontext.setLogLevel("ERROR");

    // $example on$
    // Loads data.
    Dataset<Row> dataset = spark.read().format("libsvm").load("sample_kmeans_data.txt");

    // Trains a k-means model.
    KMeans kmeans = new KMeans().setK(2).setSeed(1L);
    KMeansModel model = kmeans.fit(dataset);

    // Make predictions
    Dataset<Row> predictions = model.transform(dataset);

    // Evaluate clustering by computing Silhouette score
    ClusteringEvaluator evaluator = new ClusteringEvaluator();

    double silhouette = evaluator.evaluate(predictions);
    System.out.println("Silhouette with squared euclidean distance = " + silhouette);

    // Shows the result.
    Vector[] centers = model.clusterCenters();
    System.out.println("Cluster Centers: ");
    for (Vector center: centers) {
      System.out.println(center);
    }
    // $example off$

    spark.stop();
  }
}