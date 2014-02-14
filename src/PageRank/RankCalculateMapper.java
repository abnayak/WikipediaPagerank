package PageRank;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class RankCalculateMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

        //assuming each line has page title, rank and list of outlinks
        String svalue = value.toString();
        String[] parts = svalue.split("\t");

        String title = parts[0];
        String rank = parts[1];

        String outlinks = "";
        String rankOutlinks = "";
        int outlinkCount = 0;
        double rankVote=0.0;

        //If no of parts > 2 then only we have outlinks otherwise its null
        if ( parts.length > 2){
            int titleEndIndex = svalue.indexOf("\t");
            int rankEndIndex = svalue.indexOf("\t",titleEndIndex+1);

            //find the outlink string
            outlinks = svalue.substring(rankEndIndex+1);
            outlinkCount = parts.length - 2;
            rankOutlinks += "\t" + outlinks;
            rankVote = Double.parseDouble(rank)/outlinkCount;
        }

        //Send this to notify if the page is a existing page or not(to avoid red links)
        output.collect(new Text(title), new Text("$\t"));

        //Reconstruct the page title, rank and outlinks
        if ( rankOutlinks.equals("") ){
            //This signifies this page is sink
            output.collect(new Text(title), new Text("#"));
        }else{
            output.collect(new Text(title), new Text("#" + rankOutlinks));
        }

        for(int i=2; i < parts.length ; i++){
            //send the vote and page to reducer
            output.collect(new Text(parts[i]), new Text(Double.toString(rankVote)));
        }
    }
}