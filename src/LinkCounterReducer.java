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


public class LinkCounterReducer extends MapReduceBase implements Reducer<LongWritable, NullWritable, LongWritable, NullWritable> {

    public void reduce(LongWritable key, Iterator<NullWritable> values, OutputCollector<LongWritable, NullWritable> output, Reporter reporter) throws IOException {

        long count = 1;

        while(values.hasNext()){
            values.next();
            count++;
        }

        LongWritable result = new LongWritable(count);

        output.collect(result, NullWritable.get());
    }
}
