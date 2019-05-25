import java.io.*;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Arrays;
import java.util.regex.Pattern;

import com.sun.xml.internal.bind.v2.TODO;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.functions.*;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import scala.Tuple2;
import scala.collection.mutable.WrappedArray;

import static org.apache.spark.sql.functions.*;

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
         //Dataset<Row> df2 = df.select(col("title"), col("id"), col("revision"));
         //getMinorTagsCount(df);
         //getAtMostFiveUrlLinks(df);
        getOnePlusContributorAlongWithRevisionId(df);
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

    static void getOnePlusContributorAlongWithRevisionId(Dataset<Row> dataset){
        Dataset<Row> df = (Dataset<Row>) dataset.select(dataset.col("revision")).toDF();

        df.persist();

        Dataset<Row> filteredDf = df.select(df.col("revision.id").as("rev-id"), df.col("revision.timestamp").as("rev-ts"),
                df.col("revision.contributor.id").as("contrib-id"), df.col("revision.contributor.ip").as("contrib-ip"),
                df.col("revision.contributor.username").as("contrib-u-name")).toDF();

        df.unpersist();
        filteredDf.persist();

        //for sample xml
        //filtered 2901 after grouping by contrib-id | contrib-u-name
        //         958                    contrib-ip
        //         3859                   together
        // >1      833
        Dataset<Row> filteredDfWithCount = (Dataset<Row>) filteredDf.withColumn("count", org.apache.spark.sql.functions.lit(1));

        filteredDf.unpersist();
        filteredDfWithCount.persist();

        Dataset<Row> gt1ContributionDF = (Dataset<Row>) filteredDfWithCount.groupBy(filteredDfWithCount.col("contrib-id"),
                filteredDfWithCount.col("contrib-u-name"),filteredDfWithCount.col("contrib-ip"))
                .agg(sum(filteredDfWithCount.col("count")), collect_list("rev-ts").as("rev-ts-list"),
                        collect_list("rev-id").as("rev-id-list")).toDF();

        filteredDfWithCount.unpersist();
        gt1ContributionDF.persist();

        Dataset<Row> result =  (Dataset<Row>) gt1ContributionDF.filter("sum(count) > 1").toDF();

        gt1ContributionDF.unpersist();
        result.persist();

        //TODO:Sort the revision id based on the descending order of timestamp
        //result.toJavaRDD().map(row -> sortRevision(row));
    }

    private static Row sortRevision(Row row){
        String[] timeStampArr = (String[]) row.getAs(4);
        Long[] revisionIdArr = (Long[]) row.getAs(5);
        DateTime[] dateTimesArr = new DateTime[timeStampArr.length];
        DateTimeFormatter format = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");

        HashMap<DateTime, Integer> tsIndexMap = new HashMap<>();
        for(int index = 0; index < timeStampArr.length; index++){
            dateTimesArr[index] = format.parseDateTime(timeStampArr[index]);
            tsIndexMap.put(dateTimesArr[index], index);
        }

        Arrays.sort(dateTimesArr, new Comparator<DateTime>(){
            public int compare(DateTime d1, DateTime d2){
                return d2.compareTo(d1);
            }
        });

        Long[] revisionSortedArr = new Long[timeStampArr.length];
        for(int index = 0; index < timeStampArr.length; index++){
             int oldIndex = tsIndexMap.get(dateTimesArr[index]);
             revisionSortedArr[index] = revisionIdArr[oldIndex];
        }

        return row;
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
