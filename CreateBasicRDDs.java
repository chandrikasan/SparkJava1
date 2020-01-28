import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;

public class CreateBasicRDDs {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkTraining");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //Create RDD of Integers - Method 1
        JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 2, 2, 4, 5, 6, 1, 4, 3, 4, 5, 6, 7, 8));
        System.out.println(rdd1.collect());

        //Create RDD of Integers - Method 2
        ArrayList<Integer> arrayList = new ArrayList<>(100);

        for (int i = 0; i < 100; i++) {
            arrayList.add(i);
        }
        JavaRDD<Integer> integerJavaRDD = sc.parallelize(arrayList);
        System.out.println(integerJavaRDD.collect());

        //Create RDD of String
        JavaRDD<String> strRDD1 = sc.parallelize(Arrays.asList("Welcome to SanKir Technologies", "Hello India","Good Evening","Hello World", "Good Morning"));
        System.out.println(strRDD1.collect());

        // Create RDD from file
        JavaRDD<String> strRDD = sc.textFile("C:\\student.txt");

        System.out.println(strRDD.collect());

    }
}
