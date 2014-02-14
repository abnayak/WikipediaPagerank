package PageRank;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;


public class RankCalculateMapperStage1 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    public static String count;

    public void configure(JobConf jobConf){
        count  = jobConf.get("count");
    }

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String svalue = value.toString();
        int titleEndIndex = svalue.indexOf("\t");
        String title = svalue.substring(0,titleEndIndex);

        String links = svalue.substring(titleEndIndex + 1);

        output.collect(new Text(title), new Text(links));
    }
}
