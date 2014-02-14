package PageRank;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class LinkCountMapper extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, NullWritable> {

    public void map(LongWritable key, Text value, OutputCollector<LongWritable, NullWritable> output, Reporter reporter) throws IOException {

        //String page = value.toString();
        final LongWritable one = new LongWritable(1);

        //String title = "";

        output.collect(one, NullWritable.get());
    }
}
