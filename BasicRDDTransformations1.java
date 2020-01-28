import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.*;

public class BasicRDDTransformations1 {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkTraining");
        JavaSparkContext sc = new JavaSparkContext(conf);

        rddOfIntegers(sc);
        rddOfString(sc);
        rddFromFile(sc);

        Scanner scanner = new Scanner(System.in);
        System.out.println("Enter q to quit:");
        String s = scanner.next();
    }

    public static void rddOfIntegers(JavaSparkContext sc) {
        //Create RDD of Integers - Method 1
        JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 2, 2, 4, 5, 6, 1, 4, 3, 4, 5, 6, 7, 8));
        System.out.println(rdd1.collect());


        //Create RDD of Integers - Method 2
        ArrayList<Integer> arrayList = new ArrayList<>(100);

        for (int i = 0; i < 100; i++) {
            arrayList.add(i);
        }
        JavaRDD<Integer> integerJavaRDD = sc.parallelize(arrayList);
        // Collect Action
        System.out.println(integerJavaRDD.collect());
        //Transformations - Map - On RDD of integers
        JavaRDD<Integer> squareRDD = integerJavaRDD.map((Function<Integer, Integer>) x -> x * x);
        System.out.println("squareRDD: Squares of numbers: ");
        System.out.println(squareRDD.collect());

        //Transformations - flatMap - On RDD of integers
        JavaRDD<Integer> rdd = integerJavaRDD.flatMap(new FlatMapFunction<Integer, Integer>() {
            @Override
            public Iterator<Integer> call(Integer i) throws Exception {
                ArrayList<Integer> al = new ArrayList<>();
                al.add(i-1);
                al.add(i);
                al.add(i+1);
                return al.iterator();
            }
        });

        //Transformations - Filter - On RDD of integers
        JavaRDD<Integer> evenRDD = squareRDD.filter((Function<Integer, Boolean>) i -> (i % 2 == 0));
        System.out.println("RDD with even numbers: ");
        System.out.println(evenRDD.collect());

        //Transformations - Distinct -On RDD of integers
        System.out.println("Distinct transformation:");
        System.out.println(rdd1.distinct().collect());

    }

    public static void rddOfString(JavaSparkContext sc) {
        //Create RDD of String
        JavaRDD<String> strRDD1 = sc.parallelize(Arrays.asList("Welcome to SanKir Technologies", "Hello India","Good Evening","Hello World", "Good Morning"));

        // Transformations - Map - On RDD of String
        JavaRDD<String> strRDD2 = strRDD1.map(x -> "STRING : " + x);
        System.out.println(strRDD2.collect());

        //Transformations - FlatMap - On RDD of String
        System.out.println("flatMap Example:");
        JavaRDD<String> strRDD3 = strRDD1.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                String[] words = s.split(" ");
                ArrayList<String> strList = new ArrayList<>();

                for ( String word : words) {
                    strList.add(word);
                }
                return strList.iterator();
            }
        });
        System.out.println(strRDD3.collect());

        //Transformations - filter - On RDD of String

        JavaRDD<String> strRDD4 = strRDD3.filter( x -> x.startsWith("H"));
        System.out.println(strRDD4.collect());

        //Transformations - Distinct - On RDD of String

        System.out.println("Distinct on a String RDD:");
        System.out.println(strRDD3.distinct().collect());

    }

    public static void rddFromFile(JavaSparkContext sc) {
        // Create RDD from file
        JavaRDD<String> strRDD = sc.textFile("C:\\student.txt");

        System.out.println(strRDD.collect());
        System.out.println(strRDD.count());

        //Transformations - Map
        JavaRDD<String> lineRdd = strRDD.map(x -> "Line : " + x);
        System.out.println(lineRdd.collect());

        //Transformations - flatMap
        JavaRDD<String> wordsRDD = strRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                String[] words = s.split(" ");
                ArrayList<String> stringArrayList = new ArrayList<>();
                stringArrayList.addAll(Arrays.asList(words));
                return stringArrayList.iterator();
            }
        });

        //Transformations - filter

        JavaRDD<String> wordsRDD2 = wordsRDD.filter((Function<String, Boolean>) s -> s.startsWith("i"));
        wordsRDD2.foreach((VoidFunction<String>) s -> System.out.println(s));

        //Transformations - distinct
        System.out.println(wordsRDD.collect());
        System.out.println("Distinct words from student.txt : ");
        System.out.println(wordsRDD.distinct().collect());
        wordsRDD.distinct().saveAsTextFile("C:\\Users\\Chandrika Sanjay\\wordsRDD");


    }
}
