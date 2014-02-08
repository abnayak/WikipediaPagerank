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


public class InlinkMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    private static final Pattern wikiLinksPattern = Pattern.compile("\\[.+?\\]");

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {


        String page = value.toString();
        String txt = "";
        String title = "";

        title = page.substring(page.indexOf("<title>")+7, page.indexOf("</title>")).replace(" ", "_");
        txt = page.substring(page.indexOf("<text"), page.indexOf("</text>"));
//        txt = txt.substring(txt.indexOf(">"));
        Pattern p = Pattern.compile("\\[\\[(.*?)\\]\\]");
        Matcher m = p.matcher(txt);

        while(m.find()) {
//            System.out.println(m.group(1));
            String temp = m.group(1);

            if(temp != null & !temp.isEmpty() && temp.charAt(0) != '#' && temp.charAt(0) != ',' && temp.charAt(0) != '.' && temp.charAt(0) != '&'
                    && temp.charAt(0) != '\\' && temp.charAt(0) != '-' && temp.charAt(0) != '{'
                    && !temp.contains("&") && !temp.contains(":") && !temp.contains(","))
            {
                if(temp.contains("|"))
                    output.collect(new Text(title), new Text(temp.substring(0, temp.indexOf("|")).replace(" ", "_")) );
                else
                    output.collect(new Text(title), new Text(temp.replace(" ","_")) );
            }
        }

//        //Get Title and text section of one page
//        String[] TitleText = parseTitleAndText(value);
//
//        Text title = new Text((TitleText[0]).replace(" ", "_"));
//
//        //-----------------------------
//        Matcher matcher = wikiLinksPattern.matcher(TitleText[1]);
//
//        //StringBuffer outlinks = new StringBuffer();
//
//        //Loop through the matched links in [CONTENT]
//        while (matcher.find()) {
//            String outlink = matcher.group();
//
//            outlink = getWikiPageFromLink(outlink);
//
//            if(outlink == null || outlink.isEmpty()){
//              //  continue;
//                outlink = "";
//            }
//            else{
//                outlink = outlink.replace(" ", "_");
//            }
//
//            output.collect(title, new Text(outlink));
//            //outlinks.append("\t" + otherPage);
//        }

//        if (outlinks.length() != 0){
//            output.collect(title,new Text("1.0\t" + outlinks.toString() ));
//
//            //clear the string buffer
//            outlinks.delete(0, outlinks.length());
//        }
    }

    private boolean notValidPage(String pageString) {
        return pageString.contains(":");
    }

    private String getWikiPageFromLink(String aLink){
        if(isNotWikiLink(aLink)) return null;

        int start = aLink.startsWith("[[") ? 2 : 1;
        int endLink = aLink.indexOf("]");

        int pipePosition = aLink.indexOf("|");
        if(pipePosition > 0){
            endLink = pipePosition;
        }

        int part = aLink.indexOf("#");
        if(part > 0){
            endLink = part;
        }

        aLink =  aLink.substring(start, endLink);
        aLink = aLink.replaceAll("\\s", "_");
        aLink = aLink.replaceAll(",", "");
        aLink = sweetify(aLink);

        return aLink;
    }

    private String sweetify(String aLinkText) {
        if(aLinkText.contains("&amp;"))
            return aLinkText.replace("&amp;", "&");

        return aLinkText;
    }

    private String[] parseTitleAndText(Text value) throws CharacterCodingException {
        String[] titleAndText = new String[2];

        int start = value.find("<title>");
        int end = value.find("</title>", start);
        start += 7; //add <title> length.

        titleAndText[0] = Text.decode(value.getBytes(), start, end-start);

        start = value.find("<text");
        start = value.find(">", start);
        end = value.find("</text>", start);
        start += 1;

        if(start == -1 || end == -1) {
            return new String[]{"",""};
        }

        titleAndText[1] = Text.decode(value.getBytes(), start, end-start);

        return titleAndText;
    }

    private boolean isNotWikiLink(String aLink) {
        int start = 1;
        if(aLink.startsWith("[[")){
            start = 2;
        }

        if( aLink.length() < start+2 || aLink.length() > 100) return true;
        char firstChar = aLink.charAt(start);

        if( firstChar == '#') return true;
        if( firstChar == ',') return true;
        if( firstChar == '.') return true;
        if( firstChar == '&') return true;
        if( firstChar == '\'') return true;
        if( firstChar == '-') return true;
        if( firstChar == '{') return true;

        if( aLink.contains(":")) return true; // Matches: external links and translations links
        if( aLink.contains(",")) return true; // Matches: external links and translations links
        if( aLink.contains("&")) return true;

        return false;
    }
}
