
import java.util.*;
import java.util.stream.*;
import java.io.Serializable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


//To note, most of this is unchanged from previous assignment, excluding implements Serializable on classes, the countTemperatures method and the input temperature at the bottom of the main method.
public class WeatherStation implements Serializable
{

   
    //List of stations
    public static List<WeatherStation> stations = new ArrayList<WeatherStation>();
    //String to hold our city
    public String city;
    //List of measurements
    public List<Measurement> measurements = new ArrayList<Measurement>();
    
    public static void main(String[] args)
    {
        //Test Data creation

        //New list for Measurements from Limerick station,
        //New WeatherStation, Station1 in Limerick, with reference to its Measurements list.
        List<Measurement> LimerickMeasure = new ArrayList<>();
        WeatherStation Station1 = new WeatherStation("Limerick", LimerickMeasure);

        Measurement Limerick1 = new Measurement(9.5, 10); //(Temp, Time)
        Measurement Limerick2 = new Measurement(11.5, 14);
        Measurement Limerick3 = new Measurement(16.0, 18);
        Measurement Limerick4 = new Measurement(3.0, 21);

        LimerickMeasure.add(Limerick1);
        LimerickMeasure.add(Limerick2);
        LimerickMeasure.add(Limerick3);
        LimerickMeasure.add(Limerick4);

        //Add to our station
        stations.add(Station1);

        //Repeat of above for new station/different measurements
        List<Measurement> DingleMeasure = new ArrayList<>();
        WeatherStation Station2 = new WeatherStation("Dingle", DingleMeasure);

        Measurement Dingle1 = new Measurement(14.0, 9);
        Measurement Dingle2 = new Measurement(31.5, 13); //Scorcher!
        Measurement Dingle3 = new Measurement(22.0, 18);
        Measurement Dingle4 = new Measurement(16, 21);

        DingleMeasure.add(Dingle1);
        DingleMeasure.add(Dingle2);
        DingleMeasure.add(Dingle3);
        DingleMeasure.add(Dingle4);

        //Add to our station
        stations.add(Station2);

        //Repeat of above for new station/different measurements
        List<Measurement> GalwayMeasure = new ArrayList<>();
        WeatherStation Station3 = new WeatherStation("Galway", GalwayMeasure);
        
        Measurement Galway1 = new Measurement(-4.0, 7);
        Measurement Galway2 = new Measurement(3.5, 11); //Scorcher!
        Measurement Galway3 = new Measurement(6.0, 17);
        Measurement Galway4 = new Measurement(-1.5, 23);
        
        GalwayMeasure.add(Galway1);
        GalwayMeasure.add(Galway2);
        GalwayMeasure.add(Galway3);
        GalwayMeasure.add(Galway4);
        
        //Add to our station
        stations.add(Station3);

        //End of test data

        //Introduce our input temperature.
        countTemperatures(15); 
    }

    public static void countTemperatures(double temperature)
    {
        //Spark Config
        System.setProperty("hadoop.home.dir", "C:/winutils");
        SparkConf sparkConf = new SparkConf()
        .setAppName("WeatherStation")
        //Making use of 4 cores
        .setMaster("local[4]").set("spark.executor.memory", "1g");

        //Initialize our connection to the spark cluster as configured above.
        JavaSparkContext sparkcontext = new JavaSparkContext(sparkConf);

        //Hide logs for console visiblity
        sparkcontext.setLogLevel("ERROR");
            
        JavaRDD<Object> tempRange  = sparkcontext.parallelize(stations)
            //Same approach as assignment 2, where we map and stream our measurements, then filtering based on the requirements. (Assignment specified +-1)
            .map(stations -> stations.measurements.stream()
            .filter(e -> temperature +1 >= e.getTemperature())
            .filter(e -> temperature -1 <= e.getTemperature())
            //Map based on the .getTemperature, leaving just our values +-1 of the input temperature.
            .map(e -> e.getTemperature())
            //.collect(Collectors.groupingBy(e -> e, Collectors.counting())));
            //We put these values into a list for counting. (Above was an attempt to do this all within the RDD, but produced a hashmap, making more awkward for future use)
            .collect(Collectors.toList()));

        //Init count/result.
        int result = 0;

        //Counter for the JavaRDD as I couldn't get a filter for empty values to work.
        //We use .collect to get the data inside tempRange.
        for(Object line:tempRange.collect())
        {
            //For each entry in tempRange, we check for empties.
            if(!((ArrayList)line).isEmpty())
            {
                //If they are empty, we +1 to result for each value.
                for(int i = 0; ((ArrayList)line).size() > i; i++)
                {
                    //System.out.println(((ArrayList)line).get(i));
                    result++;
                }
            }
        }

//Print out for temp and count within the given range (+-1 degree).
        System.out.println("####################################################\n" 
        +"####################################################\n" 
        + "Temperature: " + temperature + ", Count: " + result
        + "\n" + "####################################################"
        + "\n" + "####################################################");

        //Shutdown our spark cluster.
        sparkcontext.stop();
		sparkcontext.close();
    }

        //Generated constructor for our test data
    public WeatherStation(String city, List<Measurement> measurements)
    {
        this.city = city;
        this.measurements = measurements;
    }

    //Getters and setters for city/measurements
    public String getCity()
    {
        return city;
    }

    public void setCity(String city)
    {
        this.city = city;
    }

    public List<Measurement> getMeasurements()
    {
        return measurements;
    }

    public void setMeasurements(List<Measurement> measurements)
    {
        this.measurements = measurements;
    }
    
}

class Measurement implements Serializable
{
    public double temperature;
    public int time;

    public Measurement(double temperature, int time)
    {
        this.time = time;
        this.temperature = temperature;
    }

    //Getters and Setters for temp/time
    public double getTemperature()
    {
        return temperature;
    }

    public void setTemperature(double temperature)
    {
        this.temperature = temperature;
    }
}