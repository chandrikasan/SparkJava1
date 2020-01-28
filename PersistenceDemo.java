import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import java.util.Arrays;
import java.util.Calendar;

public class PersistenceDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkTraining");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1,2,3,4,4,5,6,6,7,8,7,2));
        System.out.println("*************** Actions without Persistence ***************");
        persistExample(rdd1, false);
        System.out.println("***************  Actions with Persistence *************** ");
        persistExample(rdd1, true);
    }

    public static void persistExample(JavaRDD<Integer> rdd1, boolean flag) {
        //Persistence (caching)
        long startTime = Calendar.getInstance().getTimeInMillis();
        System.out.println("Start Time: " + startTime);
        if (flag)
            rdd1.persist(StorageLevel.MEMORY_ONLY());
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
        long endTime = Calendar.getInstance().getTimeInMillis();
        System.out.println("End Time: " + endTime);

        if (flag)
            rdd1.unpersist();

        System.out.println("Time taken :  " + (endTime - startTime));
    }
}
