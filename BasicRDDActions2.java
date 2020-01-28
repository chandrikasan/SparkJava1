import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import java.util.Arrays;

public class BasicRDDActions2 {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkTraining");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1,2,3,4,5,6,7,8), 4);
        System.out.println("Number of partitions: " + rdd1.getNumPartitions());


        Integer foldResult = rdd1.fold(5, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i, Integer j) throws Exception {
                System.out.println(" i: " + i + " j: " + j  );
                return i+j;
            }
        });

        System.out.println(foldResult);


    }
}
