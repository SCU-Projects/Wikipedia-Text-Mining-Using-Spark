import java.io.*;
import java.util.regex.Pattern;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import static org.apache.spark.sql.functions.*;

public class WikipediaTextMining {

    private static final Pattern SPACE = Pattern.compile(" ");
    private static final String basePath = "F:\\BigData\\Assignments\\assignment-3";

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: Java-Wikipedia-Data-Mining <file>");
            System.exit(1);
        }
        PrintStream fileOut = new PrintStream(basePath + "\\src\\main\\resources" + "\\output-3.txt");
        System.setOut(fileOut);

        SparkConf sparkConf = new SparkConf().setAppName("Java-Wikipedia-Data-Mining")
                .setMaster("local");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        SparkSession spark = SparkSession.builder().getOrCreate();
        boolean sample = true;
        Dataset<Row> df = spark.read()
                .format("xml")
                .option("rowTag", "page")
                .load(basePath + (sample ? "\\src\\main\\resources" : "")  + (!sample ? "\\Wiki_data_dump_32GB.xml" : "\\sample.xml"));
        driver(df);
        ctx.stop();
        System.exit(0);
    }

    static void driver(Dataset<Row> df){
        int action = 1;
        switch(action){
            case 1: getMinorTagsCount(df);
                    break;
            case 2: getAtMostFiveUrlLinks(df);
                    break;
            case 3: getOnePlusContributorAlongWithRevisionId(df);
                    break;
        }
    }

    static long getMinorTagsCount(Dataset<Row> dataset){
        long count = dataset.filter(String.valueOf(dataset.col("revision").getField("minor").isNotNull())).count();
        System.out.println("Minor tags count:"+ count);
        return count;
    }

    static void getAtMostFiveUrlLinks(Dataset<Row> dataset){
        Dataset<Row> df = dataset.select(dataset.col("id").as("page-id"), dataset.col("title").as("page-title"),
                dataset.col("revision.text").as("rev-text")).toDF();

        Dataset<Row> filteredRows = (Dataset<Row>) df.filter(row -> Utilities.isMaxFiveUrlsInTextRegex(row)).select("page-id","page-title");
        System.out.println(String.format("%-10s \t\t %-15s","Page-id", "Page-title"));
        System.out.println("-----------------------------------------------------------------------");
        filteredRows.foreach(row ->
                System.out.println(String.format("%-10s \t\t %-15s",row.getAs("page-id").toString(), row.getAs("page-title").toString())));
    }

    static void getOnePlusContributorAlongWithRevisionId(Dataset<Row> dataset){
        Dataset<Row> df = (Dataset<Row>) dataset.select(dataset.col("revision")).toDF();

        //df.persist();

        Dataset<Row> filteredDf = df.select(df.col("revision.id").as("rev-id"), df.col("revision.timestamp").as("rev-ts"),
                df.col("revision.contributor.id").as("contrib-id"), df.col("revision.contributor.ip").as("contrib-ip"),
                df.col("revision.contributor.username").as("contrib-u-name")).toDF();

        //df.unpersist();
        //filteredDf.persist();

        //for sample xml
        //filtered 2901 after grouping by contrib-id | contrib-u-name
        //         958                    contrib-ip
        //         3859                   together
        // >1      833
        Dataset<Row> filteredDfWithCount = (Dataset<Row>) filteredDf.withColumn("count", org.apache.spark.sql.functions.lit(1));

        //filteredDf.unpersist();
        //filteredDfWithCount.persist();

        Dataset<Row> gt1ContributionDF = (Dataset<Row>) filteredDfWithCount.groupBy(filteredDfWithCount.col("contrib-id"),
                filteredDfWithCount.col("contrib-u-name"),filteredDfWithCount.col("contrib-ip"))
                .agg(sum(filteredDfWithCount.col("count")), collect_list("rev-ts").as("rev-ts-list"),
                        collect_list("rev-id").as("rev-id-list")).toDF();

        //filteredDfWithCount.unpersist();
        //gt1ContributionDF.persist();

        Dataset<Row> result =  (Dataset<Row>) gt1ContributionDF.filter("sum(count) > 1").toDF();

        //gt1ContributionDF.unpersist();
        //result.persist();

        //TODO:Test it for 32GB
        //result.toJavaRDD().map(row -> sortRevision(row));
        System.out.println(String.format("%-35s \t\t %-40s","Contributor", "Revision-id"));
        System.out.println("-----------------------------------------------------------------------");
        result.foreach(row -> Utilities.sortAndAddToOutput(row));
    }

}
