import scala.Tuple2;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.mllib.regression.LabeledPoint;
import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class sparkSVD
{
    public static void main(String[] args)
    {
        //Spark Config
        System.setProperty("hadoop.home.dir", "C:/winutils");
        SparkConf sparkConf = new SparkConf()
        .setAppName("WeatherStation")
        //Making use of 4 cores
        .setMaster("local[4]").set("spark.executor.memory", "1g");

//Question 1:



        //Initialize our connection to the spark cluster as configured above.
        JavaSparkContext sparkcontext = new JavaSparkContext(sparkConf);

        //Hide logs for console visiblity
        sparkcontext.setLogLevel("ERROR");

        //File path
        String path = "imdb_labelled.txt";

        //RDD creation, reading in our imdb_labelled.txt
        JavaPairRDD<Integer, String> lines = sparkcontext.textFile(path)
            //Mapping our values based on the "\\t" (tab) found in the .txt file.
            .map(e -> e.split("\\t"))
            //Create a tuple with 2 elements, the first is either a 0/1 (as seen in the .txt) used for classification.
            //The second is the string contained in the .txt
            .mapToPair(e -> new Tuple2<Integer,String>(Integer.parseInt(e[1]), e[0]));

        /*
        for(Tuple2<Integer, String> line:lines.collect())
        {
            System.out.println(line);
        }
        */

        //num of features
        final HashingTF tf= new HashingTF(15000);

        //LabeledPoint required for train/test. SVM required each feature as a vector, we can do this via tf.transform,
        //splitting on each space.
        JavaRDD<LabeledPoint> data = lines.map(e-> new LabeledPoint(e._1(), tf.transform(Arrays.asList(e._2().split(" ")))));

        //Random split into test/train data as per assignment spec (60% train, 40% test).
        //123L is a long, representing the random seed.
        JavaRDD<LabeledPoint>[] splits = data.randomSplit(new double[]{0.6, 0.4}, 123L);
        
        //Taking the above random split and assigning to train RDD.
        //We keep the train set in memory for future reference.
        JavaRDD<LabeledPoint> train = splits[0].cache();

        //Init and run our SVM with 10000 runs on the train set.
        SVMModel svm = SVMWithSGD.train(train.rdd(), 10000);

        //Clear threshold to allow raw prediction scores to be output by predict.
        svm.clearThreshold();

        //Again using the previous JavaRDD<LabeledPoint>[] splits we assign to test RDD.
        JavaRDD<LabeledPoint> test = splits[1];

        //We will now use the test set to output an accuracy via .predict()
        //A new JavaPairRDD is created, we can mapToPair our test data to a new tuple containing the predeicted labels and features.
        JavaPairRDD<Object, Object> predictionAndLabels = test.mapToPair(e ->new Tuple2<>(svm.predict(e.features()), e.label()));

        //predictionAndLabels is an RDD of prediction, label tuple as stated above, we can iterate through this with foreach(), printing the vector and classification.
        predictionAndLabels.foreach(e -> {System.out.println("Classification for feature " +e._1() + " is equal to: " + e._2());}); 

//Question 2:

        //mllib let us import multiple metrics for use on our RDD (in this case, test set).
        //With this we can create the below evaluator.
        BinaryClassificationMetrics metrics = new BinaryClassificationMetrics(predictionAndLabels.rdd());

        //We can use .areaUnderROC() to get our AUROC and print.
        System.out.println("\nAUROC: " + metrics.areaUnderROC());

        //We get a result of .7291... In my opinion this is pretty good and represents that our accuracy 
        //is pretty good, 1.0 being 100% accurate.

        //Shutdown our spark cluster.
        sparkcontext.stop();
		sparkcontext.close();
    }
}