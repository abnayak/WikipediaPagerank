package PageRank;

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

public class RankCalculateReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    public static String count;

    public void configure(JobConf jobConf){
        count  = jobConf.get("count");
    }

    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

        int N = Integer.parseInt(count);

        String title = key.toString();
        double rank = 0.0;
        double votes = 0.0;
        double d = 0.85f;
        String outlinks = "";
        boolean redLink=true;
        boolean pageWithoutOutlinks = false;

        while(values.hasNext()){
            String svalue = values.next().toString();

            if ( svalue.indexOf("\t") != -1){

                String[] svalues = svalue.split("\t");

                //Received symbol constructs the outlinks of the page
                if (svalues[0].equals("#")){
                    for(int i= 1; i < svalues.length; i++){
                        outlinks += "\t" + svalues[i];
                    }
                    continue;
                }

                //Received symbol signifies that the page is valid page
                if (svalues[0].equals("$")){
                    redLink = false;
                    continue;
                }

            }else{

                if (svalue.equals("#")){
                    pageWithoutOutlinks = true;
                }else{
                    votes += Double.parseDouble(svalue);
                }

            }
        }

        if (!redLink){
            rank = (0.15/N) + d * votes ;

            //If the page is a sink only print the title and rank
            if (pageWithoutOutlinks){
                output.collect(new Text(title), new Text ( Double.toString(rank)  ) );
            }else{ //else print the title, rand and outlinks
                output.collect(new Text(title), new Text ( Double.toString(rank) + outlinks ) );
            }
        }
    }
}