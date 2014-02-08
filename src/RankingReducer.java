/**
 * Created by dhanu on 02/02/14.
 */
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class RankingReducer extends MapReduceBase implements Reducer<DoubleWritable, Text, DoubleWritable, Text> {

    public void reduce(DoubleWritable key, Iterator<Text> values, OutputCollector<DoubleWritable, Text> output, Reporter reporter) throws IOException {
        String page = "";
        while ( values.hasNext()){
            output.collect(new DoubleWritable(Double.parseDouble(key.toString())* -1), new Text(values.next().toString()));
        }
    }
}