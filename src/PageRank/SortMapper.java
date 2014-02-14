package PageRank;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class SortMapper extends MapReduceBase implements Mapper<LongWritable, Text, DoubleWritable, Text> {

    public void map(LongWritable key, Text value, OutputCollector<DoubleWritable, Text> output, Reporter reporter) throws IOException {

        String svalues = value.toString();

        int titleEndIndex = svalues.indexOf("\t");
        int rankEndIndex = svalues.indexOf( "\t", titleEndIndex+1);

        if ( rankEndIndex == -1){
            rankEndIndex = svalues.length();
        }

        String srank = svalues.substring(titleEndIndex+1,rankEndIndex);

        double rank = Double.parseDouble(srank);

        output.collect(new DoubleWritable(Double.parseDouble(srank)), new Text(svalues.substring(0,titleEndIndex)));
    }
}