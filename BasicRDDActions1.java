import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;

public class BasicRDDActions1 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkTraining");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1,2,1,3,4,3,4,5,6));

        // Actions - Reduce to find sum of elements in RDD
        Integer sum = rdd1.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer a, Integer b) throws Exception {
                System.out.println("a: " + a + " b: " + b + " a + b: " + (a + b));
                return a + b;
            }
        });

        System.out.println("Sum: " + sum);

        // ACTIONS - Reduce to find product of elements in RDD
        Integer prod = rdd1.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i, Integer j) throws Exception {
                System.out.println("i: " + i + " j: " + j + " i*j:" + (i*j) );
                return i * j;
            }
        });
        System.out.println("Prod: " + prod);
        // ACTIONS - count
        System.out.println("Number of elements in RDD1: " + rdd1.count());
        // ACTIONS - first
        System.out.println("First element: " + rdd1.first());
        // ACTIONS - take
        System.out.println("First 3 elements:" + rdd1.take(3));
        // ACTIONS - top
        System.out.println("Output of top: " + rdd1.top(2));
        // ACTIONS - countByValue
        System.out.println("Count by value: " + rdd1.countByValue());
        // ACTIONS - takeOrdered
        System.out.println("takeOrdered: " + rdd1.takeOrdered(5));
        // ACTIONS - takeSample
        System.out.println("takeSample with Replacement " + rdd1.takeSample(true,6,4));
        System.out.println("takeSample without Replacement " + rdd1.takeSample(false,6,1));

    }
}
