import org.apache.spark.sql.Row;
import org.joda.time.DateTime;
import scala.collection.mutable.WrappedArray;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Utilities {
    public static Revision sortRevision(Row row){
        String[] timeStampArr = (String[])((WrappedArray)row.getAs(4)).array();
        Long[] revisionIdArr =  (Long[])  ((WrappedArray)row.getAs(5)).array();
        DateTime[] dateTimesArr = new DateTime[timeStampArr.length];

        HashMap<DateTime, Integer> tsIndexMap = new HashMap<>();
        for(int index = 0; index < timeStampArr.length; index++){
            dateTimesArr[index] = new DateTime(timeStampArr[index]);
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
        String contributorName = (String)row.getAs("contrib-u-name");

        if(contributorName == null)
            contributorName = (String)row.getAs("contrib-ip");

        return new Revision(contributorName, revisionSortedArr);
    }

    public static boolean isMaxFiveUrlsInTextRegex(Row row){
        String text = row.getAs("rev-text").toString();
        Pattern p = Pattern.compile("\\|url=");
        Matcher m = p.matcher(text);
        int count = 0;
        while (m.find()){
            count++;
            if(count > 5){
                text = null;
                return false;
            }
        }
        text = null;
        return true;
    }

    public static void sortAndAddToOutput(Row row) {
        Revision revision = sortRevision(row);
        Arrays.stream(revision.idArr)
                .forEach(revId -> System.out.println(String.format("%-35s \t\t %-40s",revision.contributor, revId)));
    }

    static class Revision{
        String contributor;
        Long[] idArr;

        Revision(String contributor, Long[] idArr){
            this.contributor = contributor;
            this.idArr = idArr;
        }
    }
}
