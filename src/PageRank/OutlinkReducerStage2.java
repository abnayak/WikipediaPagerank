package PageRank;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class OutlinkReducerStage2 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

        StringBuilder temp = new StringBuilder();

        if ( key.toString().equals("#")){
            while(values.hasNext())
            {
                output.collect(new Text(values.next()), new Text(""));
            }

        }else{
            while(values.hasNext())
            {
                String val = values.next().toString();

                if ( !val.equals("#") )
                    temp.append(val + "\t");
            }

            if (temp.length() > 0)
                temp.deleteCharAt(temp.length() - 1);

            output.collect(key, new Text(temp.toString()));
        }
    }
}