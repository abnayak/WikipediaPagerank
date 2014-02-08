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

        //Read all the elements from the set and append in the StringBuilder

//        while(i.hasNext()){
//            String newVal = (String) i.next();
//
//            //if ( count == 1 && newVal.equals("#") ){
//              //  output.collect(key, new Text("#"));
//               // break;
//            //}
//
//            if ( newVal.equals("#"))
//                isRedLink = false;
//
//            if(!isRedLink && !newVal.equals("#"))
//            {
//                output.collect(new Text(newVal), key);
//            }
//        }
//
//        if ( set.size() == 1 && !isRedLink )
//            output.collect(new Text(), key);
//

        if ( set.size() == 1){
            //if the page is a sink
            if (set.contains("#")){
                output.collect(key, new Text("#"));
            }

        }else{
            if (set.contains("#")){
                Iterator i = set.iterator();
                while ( i.hasNext()){
                    String val = (String) i.next();
                    output.collect(new Text(val), key);
                }
            }
        }

//        double rank = 1.0/6299;
//
//        if ( outlinks.length() != 0){
//            output.collect(key,new Text(Double.toString(rank)+outlinks.toString()));
//        }else{
//            output.collect(key,new Text(Double.toString(rank)));
//        }
    }
}