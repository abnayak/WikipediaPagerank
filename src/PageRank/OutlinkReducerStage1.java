package PageRank;

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

public class OutlinkReducerStage1 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

        StringBuilder outlinks = new StringBuilder();
        Set<String> set = new HashSet<String>();
        int count = 0 ;
        Boolean isRedLink = true;

        while ( values.hasNext()){
            //Add only unique links
            set.add(values.next().toString());
            count++;
        }

//        if ( set.size() == 1){
//            //if the page is a sink
//            if (set.contains("#")){
//                output.collect(key, new Text("#"));
//            }
//
//        }else{

            if (set.contains("#")){
                Iterator i = set.iterator();
                while ( i.hasNext()){
                    String val = (String) i.next();
                    if ( !val.equals("#"))
                        output.collect(new Text(val), key);
                    else {
                        output.collect( key, new Text("#"));
                    }
                }
            }

        //}
    }
}