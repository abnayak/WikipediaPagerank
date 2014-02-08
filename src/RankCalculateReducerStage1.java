import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class RankCalculateReducerStage1 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    public static String count;

    public void configure(JobConf jobConf){
        count  = jobConf.get("count");
    }

    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        float N = 1.0f / Integer.parseInt(count);
        String svalues="";

        while (values.hasNext())
            svalues = values.next().toString();

        output.collect(key, new Text(Float.toString(N) + "\t" + svalues) );
    }
}
