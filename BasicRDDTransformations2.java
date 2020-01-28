import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.util.*;

public class BasicRDDTransformations2 {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkTraining");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1,2,3,4,4,5,6,6,7,8,7,2));
        JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(3,4,5,6));

        // TRANSFORMATIONS - sample:
        System.out.println("Sample :");
        System.out.println(rdd1.sample(true,0.75).collect());
        System.out.println(rdd1.sample(false,0.75).collect());
        System.out.println(rdd1.sample(false,0.5).collect());

        // TRANSFORMATIONS - union
        System.out.println("Union: ");
        System.out.println(rdd1.union(rdd2).collect());
        System.out.println("Intersection: ");
        // TRANSFORMATIONS - intersection
        System.out.println(rdd1.intersection(rdd2).collect());
        System.out.println("Subtract: ");
        // TRANSFORMATIONS - subtract
        System.out.println(rdd1.subtract(rdd2).collect());
        System.out.println("Cartesian: ");
        // TRANSFORMATIONS - cartesian
        System.out.println(rdd1.cartesian(rdd2).collect());


        Scanner scanner = new Scanner(System.in);
        System.out.println("Enter q to quit:");
        String s = scanner.next();
    }


}
