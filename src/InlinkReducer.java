import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class InlinkReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

        StringBuilder outlinks = new StringBuilder();
        Set<String> set = new HashSet<String>();

        while ( values.hasNext()){
            //Add only unique links
            set.add(values.next().toString());
        }

        //Read all the elements from the set and append in the StringBuilder
        Iterator i = set.iterator();
        while(i.hasNext()){
            outlinks.append("\t" + i.next());
        }

        double rank = 1.0/6299;

        if ( outlinks.length() != 0){
            output.collect(key,new Text(Double.toString(rank)+outlinks.toString()));
        }else{
            output.collect(key,new Text(Double.toString(rank)));
        }
    }
}