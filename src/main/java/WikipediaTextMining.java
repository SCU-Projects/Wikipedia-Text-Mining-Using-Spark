import java.util.regex.Pattern;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import static org.apache.spark.sql.functions.col;

public class WikipediaTextMining {

    private static final Pattern SPACE = Pattern.compile(" ");


    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: JavaWordCount <file>");
            System.exit(1);
        }
        String inputFile = args[0];
        SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount")
                .setMaster("local");
        System.out.println("Spark configuration loaded");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        //JavaRDD<String> lines = ctx.textFile(inputFile, 1);

        SparkSession spark = SparkSession.builder().getOrCreate();
        Dataset<Row> df = spark.read()
                .format("xml")
                .option("rowTag", "page")
                .load("F:\\BigData\\Assignments\\assignment-3\\src\\main\\resources\\sample.xml");
         Dataset<Row> df2 = df.select(col("title"), col("id"), col("revision"));
         long count = df2.filter(String.valueOf(df2.col("revision").getField("minor").isNotNull())).count();
         System.out.println(count);
        ctx.stop();
        System.exit(0);
    }
}
