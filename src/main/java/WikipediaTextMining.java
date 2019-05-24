import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;

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
         //getMinorTagsCount(df2);
        getAtMostFiveUrlLinks(df2);
        ctx.stop();
        System.exit(0);
    }

    static long getMinorTagsCount(Dataset<Row> dataset){
        long count = dataset.filter(String.valueOf(dataset.col("revision").getField("minor").isNotNull())).count();
        System.out.println("Minor tags count:"+ count);
        return count;
    }

    static void getAtMostFiveUrlLinks(Dataset<Row> dataset){
        List<Row> filteredRows = dataset.filter(row -> isMaxFiveUrlsInTextField(row)).collectAsList();
        Row row = filteredRows.get(0);
        System.out.println("Filtered rows with <= 5 url count:" + filteredRows.size());
    }

    private static boolean isMaxFiveUrlsInTextField(Row row) {
        GenericRowWithSchema revision = row.getAs("revision");
        String text = revision.getAs("text").toString();
        String[] words = text.split(" ");
        int count = 0;
        for(String word : words){
            if(word.contains("|url="))
                count++;
            if(count > 5)
                return false;
        }
        return true;
    }

    class PageDetails{
        Integer pageId;
        String pageTitle;

        PageDetails(Integer pageId, String pageTitle){
            this.pageId = pageId;
            this.pageTitle = pageTitle;
        }
    }

}
