import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.util.StatCounter;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class NumericRDDOperations {
    public static void main(String args[]) throws ParseException {
        SparkConf spark = new SparkConf().setMaster("local").setAppName("Sankir");
        JavaSparkContext jsc = new JavaSparkContext(spark);

        JavaRDD<String> RDD1 = jsc.textFile("C:\\Users\\Chandrika Sanjay\\NumericRDDInput.txt");

        System.out.println(RDD1.collect());

        JavaRDD<Double> RDD2 = RDD1.flatMap(new FlatMapFunction<String, Double>() {
            @Override
            public Iterator<Double> call(String s) {
                String[] words = s.split(",");
                List<Double> list1 = new ArrayList<>();
                for (String str : words) {
                    list1.add(Double.parseDouble(str));
                }
                return list1.iterator();
            }
        });



        System.out.println(RDD2.collect());

        JavaDoubleRDD doubleRDD = RDD2.mapToDouble(new DoubleFunction<Double>() {
            @Override
            public double call(Double d) {
                return d;
            }
        });


        System.out.println("Count: " + RDD2.count());


        final StatCounter stats = doubleRDD.stats();

        final Double maxValue = stats.max();
        final Double minValue = stats.min();
        final Double meanValue = stats.mean();
        final Double sum = stats.sum();

        System.out.println("Max: " + maxValue);
        System.out.println("Min: " + minValue);
        System.out.printf("Avg: %.2f \n" , meanValue);
        System.out.printf("Sum: %.2f \n" , sum);

        System.out.printf("Variance: %.2f\n " , stats.variance());
    }
}
