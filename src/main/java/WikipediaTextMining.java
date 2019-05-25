import java.io.*;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;

import static org.apache.spark.sql.functions.col;

public class WikipediaTextMining {

    private static final Pattern SPACE = Pattern.compile(" ");
    private static final String basePath = "F:\\BigData\\Assignments\\assignment-3\\src\\main\\resources";

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
                .load(basePath+"\\sample.xml");
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
        JavaRDD<Row> filteredRows = (JavaRDD<Row>) dataset.filter(row -> isMaxFiveUrlsInTextField(row)).select("id","title").toJavaRDD();
        StringBuilder sb = new StringBuilder();
        List<Row> filteredRowsList = filteredRows.collect();
        filteredRowsList.stream().forEach(row -> addToOutput(sb, row));
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(basePath+"\\output.txt"));
            writer.write(sb.toString());
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        //System.out.println("Filtered rows with <= 5 url count:" + filteredRows.count());
    }

    private static void addToOutput(StringBuilder sb, Row row){
        String rowString = String.format("%s \t\t %s \n",row.getAs("id").toString(), row.getAs("title").toString());
        sb.append(rowString);
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
