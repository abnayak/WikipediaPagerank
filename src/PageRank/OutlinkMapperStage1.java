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
import java.util.Set;
import java.util.HashSet;
import java.util.Iterator;



public class OutlinkMapperStage1 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    private static final Pattern wikiLinksPattern = Pattern.compile("\\[.+?\\]");

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

        String page = value.toString();
        String txt = "";
        String title = "";
        Set<String> set = new HashSet<String>();

        title = page.substring(page.indexOf("<title>")+7, page.indexOf("</title>")).replace(" ", "_");

        int startIndex = page.indexOf("<text");
        int endIndex = page.indexOf("</text>");
        if ( startIndex != -1 && endIndex != -1)
            txt = page.substring(startIndex, endIndex );

        Pattern p = Pattern.compile("\\[\\[(.*?)\\]\\]");
        Matcher m = p.matcher(txt);

        //This part handles the red links
        output.collect(new Text(title), new Text("#"));

        while(m.find()) {

            String temp = m.group(1);

//            if(temp != null & !temp.isEmpty() && temp.charAt(0) != '#' && temp.charAt(0) != ',' && temp.charAt(0) != '.' && temp.charAt(0) != '&'
//                    && temp.charAt(0) != '\\' && temp.charAt(0) != '-' && temp.charAt(0) != '{'
//                    && !temp.contains("&") && !temp.contains(":") && !temp.contains(","))
//            {
            if(temp != null && !temp.isEmpty())
            {
                if(temp.contains("|")){
                    String outlink = temp.substring(0, temp.indexOf("|")).replace(" ", "_");
                    if ( !outlink.equals(title))
                        output.collect(new Text(outlink), new Text(title) );
                }
                else{
                    String outlink = temp.replace(" ","_");
                    if ( !outlink.equals(title))
                        output.collect( new Text(outlink), new Text(title) );
                }
            }
        }
    }
}