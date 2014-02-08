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


public class OutlinkMapperStage1 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    private static final Pattern wikiLinksPattern = Pattern.compile("\\[.+?\\]");

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

        String page = value.toString();
        String txt = "";
        String title = "";

        title = page.substring(page.indexOf("<title>")+7, page.indexOf("</title>")).replace(" ", "_");
        txt = page.substring(page.indexOf("<text"), page.indexOf("</text>"));
        Pattern p = Pattern.compile("\\[\\[(.*?)\\]\\]");
        Matcher m = p.matcher(txt);

        //This part handles the red links
        output.collect(new Text(title), new Text("#"));

        while(m.find()) {

            String temp = m.group(1);

            if(temp != null & !temp.isEmpty() && temp.charAt(0) != '#' && temp.charAt(0) != ',' && temp.charAt(0) != '.' && temp.charAt(0) != '&'
                    && temp.charAt(0) != '\\' && temp.charAt(0) != '-' && temp.charAt(0) != '{'
                    && !temp.contains("&") && !temp.contains(":") && !temp.contains(","))
            {
                if(temp.contains("|")){
                    output.collect(new Text(temp.substring(0, temp.indexOf("|")).replace(" ", "_")), new Text(title) );
                }
                else{
                    output.collect( new Text(temp.replace(" ","_")), new Text(title) );
                }
            }
        }
    }
}