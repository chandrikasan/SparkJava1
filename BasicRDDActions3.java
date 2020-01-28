import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;
import scala.Tuple4;
import java.util.Arrays;

public class BasicRDDActions3 {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkTraining");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1,2,3,4,5,6,7,8),4);


        Tuple4<String,Integer,Boolean,Float> t = new Tuple4("abc",5,true,5.5);

        Tuple2<Integer,Integer> t1 = new Tuple2<>(0,0);

        Function2<Tuple2<Integer,Integer>,Integer, Tuple2<Integer,Integer>> seqFunc = new Function2<Tuple2<Integer,Integer>,Integer, Tuple2<Integer,Integer>>() {
            @Override
            public Tuple2<Integer,Integer> call(Tuple2<Integer,Integer> t, Integer j) throws Exception {
                Integer a = t._1;
                Integer b = t._2;
                Integer sum = a + j;
                Integer count = b + 1;
                System.out.println("(" + a + "," + b + ")"+ " j: " + j +  " (" + sum + ","+ count + ")");
                return new Tuple2<>(sum, count);
            }
        };

        Function2<Tuple2<Integer,Integer>, Tuple2<Integer,Integer>, Tuple2<Integer,Integer>> combinerFunc = new Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> t1, Tuple2<Integer, Integer> t2) throws Exception {
                Integer sum = t1._1 + t2._1;
                Integer count = t1._2 + t2._2;
                System.out.println("t1: " + t1._1 + ","+ t1._2 + " t2: " + t2._1 + "," + t2._2 + " Sum: " + sum + " Count: " + count);
                return new Tuple2(sum,count);
            }
        };

        rdd1.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer i) throws Exception {
                System.out.println("Element : " + i);
            }
        });

        Tuple2<Integer,Integer> result = rdd1.aggregate(t1, seqFunc, combinerFunc );

        System.out.println(result);

        Float avg = result._1 / (float) result._2 ;

        System.out.println("Average: " + avg);

    }


}
