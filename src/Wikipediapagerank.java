import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;


public class Wikipediapagerank {

    public static int count =0;

    public static void main(String[] args) throws Exception {
        Wikipediapagerank mainObject = new Wikipediapagerank();

        int noOfIterations = 8;
        String Bucket = "results/";
        String XMLInput=args[1];
        String OutlinkOutputStage1="PageRank.outlink.stage1.out";
        String OutlinkOutput="PageRank.outlink.out";
        String LinkCounterOuput="PageRank.n.out";
        String finalOutput = "PageRanks";

        String[] iterations = new String[noOfIterations+1];
        iterations[0] = OutlinkOutput;
        for ( int i =1; i <= noOfIterations ; i++){
            iterations[i] = "PageRank.iter" + Integer.toString(i) +".out";
        }

//        if ( args.length != 3){
//            System.err.println("Usage: WikipediaInlinkGenration in out");
//        }

        //Call to the inlink genration hadoop task
        //mainObject.InlinkGenrationJob(XMLInput, Bucket + InlinkOutput);
        mainObject.OutlinkGenrationJob1(XMLInput, Bucket + OutlinkOutputStage1);
        mainObject.OutlinkGenrationJob2(Bucket + OutlinkOutputStage1, Bucket + OutlinkOutput );

        //Count the total no of pages in the xml dump file
        //mainObject.InlinkCountGenerationJob(XMLInput, Bucket + LinkCounterOuput);

        //calculate the Rank for #noOfIteration times
//        for ( int i =0; i < noOfIterations ; i++){
//            //Call the rank calculation
//            mainObject.RankCalculatorJob(Bucket+iterations[i],Bucket+iterations[i+1]);
//        }

        //Sort the pages according to their page rank
        //mainObject.SortJob(Bucket + iterations[noOfIterations], Bucket + finalOutput);
        //mainObject.runRankOrdering(Bucket + iterations[noOfIterations], Bucket + finalOutput);

    }

    public void OutlinkGenrationJob1(String input, String output) throws IOException {
        JobConf conf = new JobConf(Wikipediapagerank.class);

        conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
        conf.set(XmlInputFormat.END_TAG_KEY, "</page>");

        //Configure the inlink generation mapper
        FileInputFormat.setInputPaths(conf, new Path(input));
        conf.setInputFormat(XmlInputFormat.class);
        conf.setMapperClass(OutlinkMapperStage1.class);

        //Configure the inlink genration reducer
        FileOutputFormat.setOutputPath(conf, new Path(output));
        conf.setReducerClass(OutlinkReducerStage1.class);

        //Defince the output key and value classes
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        //Start the hadoop job
        JobClient.runJob(conf);
    }

    public void OutlinkGenrationJob2(String input, String output) throws IOException {
        JobConf conf = new JobConf(Wikipediapagerank.class);

        //Configure the inlink generation mapper
        FileInputFormat.setInputPaths(conf, new Path(input));
        conf.setMapperClass(OutlinkMapperStage2.class);

        //Configure the inlink genration reducer
        FileOutputFormat.setOutputPath(conf, new Path(output));
        conf.setReducerClass(OutlinkReducerStage2.class);

        //Defince the output key and value classes
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        //Start the hadoop job
        JobClient.runJob(conf);
    }

    public void InlinkGenrationJob(String input, String output) throws IOException {
        JobConf conf = new JobConf(Wikipediapagerank.class);

        conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
        conf.set(XmlInputFormat.END_TAG_KEY, "</page>");

        //Configure the inlink generation mapper
        FileInputFormat.setInputPaths(conf, new Path(input));
        conf.setInputFormat(XmlInputFormat.class);
        conf.setMapperClass(InlinkMapper.class);

        //Configure the inlink genration reducer
        FileOutputFormat.setOutputPath(conf, new Path(output));
        conf.setReducerClass(InlinkReducer.class);

        //Defince the output key and value classes
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        //Start the hadoop job
        JobClient.runJob(conf);
    }

    public void InlinkCountGenerationJob(String input, String output) throws IOException{
        JobConf conf = new JobConf(Wikipediapagerank.class);

        conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
        conf.set(XmlInputFormat.END_TAG_KEY, "</page>");

        //Configure the inlink generation mapper
        FileInputFormat.setInputPaths(conf, new Path(input));
        conf.setInputFormat(XmlInputFormat.class);
        conf.setMapperClass(LinkCountMapper.class);

        //Configure the inlink genration reducer
        FileOutputFormat.setOutputPath(conf, new Path(output));
        conf.setReducerClass(LinkCounterReducer.class);

        //Defince the output key and value classes
        conf.setOutputKeyClass(LongWritable.class);
        conf.setOutputValueClass(NullWritable.class);

        //Start the hadoop job
        JobClient.runJob(conf);
    }

    public void RankCalculatorJob(String input, String output) throws IOException{
        JobConf conf = new JobConf(Wikipediapagerank.class);

        //Configure the inlink generation mapper
        FileInputFormat.setInputPaths(conf, new Path(input));
        conf.setMapperClass(RankCalculateMapper.class);

        //Configure the inlink genration reducer
        FileOutputFormat.setOutputPath(conf, new Path(output));
        conf.setReducerClass(RankCalculateReducer.class);

        //Defince the output key and value classes
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        Integer count = readCountFromFile();
        conf.set("count", Integer.toString(count));

        //Start the hadoop job
        JobClient.runJob(conf);
    }

    public void SortJob(String input, String output) throws IOException{
        JobConf conf = new JobConf(Wikipediapagerank.class);

        //Configure the inlink generation mapper
        FileInputFormat.setInputPaths(conf, new Path(input));
        conf.setMapperClass(SortMapper.class);

        //Configure the inlink genration reducer
        FileOutputFormat.setOutputPath(conf, new Path(output));
        conf.setReducerClass(SortReducer.class);

        //define the output key and value from mapper
        conf.setMapOutputKeyClass(DoubleWritable.class);
        conf.setMapOutputValueClass(Text.class);

        //Define the output key and value classes from reducer
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(DoubleWritable.class);

        //Start the hadoop job
        JobClient.runJob(conf);
    }

    public void runRankOrdering(String inputPath, String outputPath) throws IOException {
        JobConf conf = new JobConf(Wikipediapagerank.class);

        conf.setOutputKeyClass(DoubleWritable.class);
        conf.setOutputValueClass(Text.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(inputPath));
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));

        conf.setMapperClass(RankingMapper.class);
        conf.setReducerClass(RankingReducer.class);
        JobClient.runJob(conf);
    }


    private Integer readCountFromFile()
    {
        String fileName = "results/PageRank.n.out/part-00000";
        Integer count = 0;
        BufferedReader br = null;
        FileSystem fs = null;
        Path path = new Path(fileName);
        try {
            fs = path.getFileSystem(new Configuration());
            br = new BufferedReader(new InputStreamReader(fs.open(path)));
            String line = br.readLine();
            if (line != null && !line.isEmpty())
            {
                count = Integer.parseInt(line);
                return count;
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            try {
                br.close();
                fs.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        return count;
    }
}