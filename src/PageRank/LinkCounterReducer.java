package PageRank;

import java.io.IOException;
import java.math.BigInteger;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.HashSet;
import java.math.BigInteger;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobConf;


public class LinkCounterReducer extends MapReduceBase implements Reducer<LongWritable, NullWritable, Text, NullWritable> {

    public void reduce(LongWritable key, Iterator<NullWritable> values, OutputCollector<Text, NullWritable> output, Reporter reporter) throws IOException {

        long count = 0;

        while(values.hasNext()){
            values.next();
            count++;
        }
        output.collect(new Text("N=" + Long.toString(count)), NullWritable.get());
    }
}