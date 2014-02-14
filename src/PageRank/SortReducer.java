package PageRank;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class SortReducer extends MapReduceBase implements Reducer<DoubleWritable, Text, Text, DoubleWritable> {

    public static String count="1.0";

    public void configure(JobConf jobConf){
        count  = jobConf.get("count");
    }

    public void reduce(DoubleWritable key, Iterator<Text> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
        int N = Integer.parseInt(count);
        double limit = 5.0/N;

        while(values.hasNext()){
            double result = Double.parseDouble(key.toString());
            //double result = currentRank - limit;
            if (result >  limit)
                output.collect(values.next(), key);
            else
               return;
        }
    }
}
