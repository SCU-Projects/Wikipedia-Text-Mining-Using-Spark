import java.io.*;
import java.time.LocalDateTime;
import java.util.regex.Pattern;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;

import static org.apache.spark.sql.functions.*;

public class WikipediaTextMining {

    private static final Pattern SPACE = Pattern.compile(" ");
    private static final String basePath = "/Users/reshmasubramaniam/Downloads/Wikipedia-Text-Mining-Using-Spark-develop /src/main/resources/Wiki_data_dump_32GB.xml";

    public static void main(String[] args) throws Exception {

        PrintStream fileOut = new PrintStream("/Users/reshmasubramaniam/Downloads/Wikipedia-Text-Mining-Using-Spark-develop /src/main/resources/output-1.txt");
        System.setOut(fileOut);

        SparkConf sparkConf = new SparkConf().setAppName("Java-Wikipedia-Data-Mining")
                .setMaster("local");

//        Use Kyro Serializer for performance tuning
//        sparkConf.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" );
//        sparkConf.set("spark.kryo.registrationRequired", "true");
//        kryo.register(org.apache.spark.sql.types.StructType.class);

        sparkConf.set("spark.executor.memory", "8g")
                .set("dynamicAllocation.enabled", "true")
                .set("maximizeResourceAllocation", "true")
                .set("defaultParallelism", "true")
                .set("spark.executor.cores", "3")
                .set("spark.cores.max", "3")
                .set("spark.driver.memory", "8g");

        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        SparkSession spark = SparkSession.builder().getOrCreate();

//        System.out.println("Started to load input file at " + LocalDateTime.now());

        Dataset<Row> df = spark.read()
                .format("xml")
                .option("rowTag", "page")
                .load(basePath);


        ctx.textFile(basePath, 1);

//        System.out.println("Successfully loaded input file at " + LocalDateTime.now());
        driver(df);
        ctx.stop();

        System.exit(0);
    }

    static void driver(Dataset<Row> df) {

        int action = 1; //Action needs to be manually updated for each cases

        switch (action) {
            case 1:
                getMinorTagsCount(df); //To determine the total number of minor revision available in the data set.
                break;
            case 2:
                getAtMostFiveUrlLinks(df); //List out the page title and page id of all the pages that have at most five URL links mentioned in their text field.
                break;
            case 3:
                getOnePlusContributorAlongWithRevisionId(df); //List out all the contributor with more than one contribution along with the revision-id. The list of revision-id must be sorted in the descending order of timestamp.
                break;
        }
    }

    static long getMinorTagsCount(Dataset<Row> dataset) {
        long count = dataset.filter(String.valueOf(dataset.col("revision").getField("minor").isNotNull())).count();
        System.out.println("Minor tags count:" + count);
        return count;
    }

    static void getAtMostFiveUrlLinks(Dataset<Row> dataset) {
//        System.out.println("Starting transformation/filter at " + LocalDateTime.now());
        System.out.println(String.format("%-10s \t\t %-15s", "Page-id", "Page-title"));
        System.out.println("-----------------------------------------------------------------------");
        dataset.select(dataset.col("id").as("page-id"),
                dataset.col("title").as("page-title"), dataset.col("revision.text").as("rev-text"))
                .filter(row -> Utilities.isMaxFiveUrlsInTextRegex(row)).select("page-id", "page-title")
                .foreach(row ->
                        System.out.println(String.format("%-10s \t\t %-15s", row.getAs("page-id").toString(), row.getAs("page-title").toString())));
//        System.out.println("Ending transformation/filter at " + LocalDateTime.now());
        //Dataset<Row> filteredRows = (Dataset<Row>) df.filter(row -> Utilities.isMaxFiveUrlsInTextRegex(row)).select("page-id","page-title");
    }

    static void getOnePlusContributorAlongWithRevisionId(Dataset<Row> dataset) {
        //Dataset<Row> df = (Dataset<Row>) dataset.select(dataset.col("revision")).toDF();

        //df.persist();
 //       System.out.println("Starting transformation at " + LocalDateTime.now());
        Dataset<Row> filteredDf = dataset.select(dataset.col("revision.id").as("rev-id"), dataset.col("revision.timestamp").as("rev-ts"),
                dataset.col("revision.contributor.id").as("contrib-id"), dataset.col("revision.contributor.ip").as("contrib-ip"),
                dataset.col("revision.contributor.username").as("contrib-u-name"))
                .withColumn("count", org.apache.spark.sql.functions.lit(1)).toDF();
//        System.out.println("Ending transformation at " + LocalDateTime.now());
        //df.unpersist();
        //filteredDf.persist();

        //for sample xml
        //filtered 2901 after grouping by contrib-id | contrib-u-name
        //         958                    contrib-ip
        //         3859                   together
        // >1      833
        //Dataset<Row> filteredDfWithCount = (Dataset<Row>) filteredDf.withColumn("count", org.apache.spark.sql.functions.lit(1));

        //filteredDf.unpersist();
        //filteredDfWithCount.persist();
//        System.out.println("Starting group by at " + LocalDateTime.now());
        System.out.println(String.format("%-35s \t\t %-40s", "Contributor", "Revision-id"));
        System.out.println("-----------------------------------------------------------------------");
        filteredDf.groupBy(filteredDf.col("contrib-id"),
                filteredDf.col("contrib-u-name"), filteredDf.col("contrib-ip"))
                .agg(sum(filteredDf.col("count")), collect_list("rev-ts").as("rev-ts-list"),
                        collect_list("rev-id").as("rev-id-list")).filter("sum(count) > 1")
                .foreach(row -> Utilities.sortAndAddToOutput(row));
//        System.out.println("Ending group by at " + LocalDateTime.now());
        //filteredDfWithCount.unpersist();
        //gt1ContributionDF.persist();

        //Dataset<Row> result =  (Dataset<Row>) gt1ContributionDF.filter("sum(count) > 1").toDF();

        //gt1ContributionDF.unpersist();
        //result.persist();

        //result.toJavaRDD().map(row -> sortRevision(row));


    }

}
