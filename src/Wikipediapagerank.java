import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.lang.ObjectUtils;
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
import org.apache.hadoop.mapred.Partitioner;


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

        String tmpLoc = "tmp/";

        String[] iterations = new String[noOfIterations+1];
        //iterations[0] = OutlinkOutput;

        for ( int i =0; i <= noOfIterations ; i++){
            iterations[i] = "PageRank.iter" + Integer.toString(i) +".out";
        }

        //Call to the in-link generation Hadoop task
        mainObject.OutlinkGenrationJob1(XMLInput, tmpLoc + OutlinkOutputStage1);
        mainObject.OutlinkGenrationJob2(tmpLoc + OutlinkOutputStage1, Bucket + OutlinkOutput );

        //Count the total no of pages in the xml dump file
        mainObject.InlinkCountGenerationJob(XMLInput, Bucket + LinkCounterOuput);

        //Convert the out-link to rank calculation format
        mainObject.RunCalculatorStage1(Bucket + OutlinkOutput, tmpLoc + iterations[0], Bucket + LinkCounterOuput);

        //calculate the Rank for #noOfIteration times
        for ( int i =1; i <= noOfIterations ; i++){
            mainObject.RankCalculatorJob(tmpLoc + iterations[i-1], tmpLoc +iterations[i], Bucket + LinkCounterOuput);
        }

        //Sort the pages according to their page rank for iteration 1 and 8
        mainObject.SortJob(tmpLoc + iterations[1], Bucket + iterations[1]);
        mainObject.SortJob(tmpLoc + iterations[1], Bucket + iterations[noOfIterations]);
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


    public void InlinkCountGenerationJob(String input, String output) throws IOException{
        JobConf conf = new JobConf(Wikipediapagerank.class);

        conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
        conf.set(XmlInputFormat.END_TAG_KEY, "</page>");

        //Configure the inlink generation mapper
        FileInputFormat.setInputPaths(conf, new Path(input));
        conf.setInputFormat(XmlInputFormat.class);
        conf.setMapperClass(LinkCountMapper.class);
        conf.setMapOutputKeyClass(LongWritable.class);
        conf.setMapOutputValueClass(NullWritable.class);

        //Configure the inlink genration reducer
        FileOutputFormat.setOutputPath(conf, new Path(output));
        conf.setReducerClass(LinkCounterReducer.class);

        //Defince the output key and value classes
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(NullWritable.class);

        //Start the hadoop job
        JobClient.runJob(conf);
    }

    public void RunCalculatorStage1 (String input, String output, String linkcountfile) throws IOException {
        JobConf conf = new JobConf(Wikipediapagerank.class);

        //Configure the inlink generation mapper
        FileInputFormat.setInputPaths(conf, new Path(input));
        conf.setMapperClass(RankCalculateMapperStage1.class);

        //Configure the inlink genration reducer
        FileOutputFormat.setOutputPath(conf, new Path(output));
        conf.setReducerClass(RankCalculateReducerStage1.class);

        //Defince the output key and value classes
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        Integer count = readCountFromFile(linkcountfile, conf);
        conf.set("count", Integer.toString(count));

        //Start the hadoop job
        JobClient.runJob(conf);
    }


    public void RankCalculatorJob(String input, String output, String linkcountfile) throws IOException{
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

        Integer count = readCountFromFile(linkcountfile, conf);
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

        //sort the keys of reducer
        conf.setOutputKeyComparatorClass(KeyComparator.class);
        conf.setOutputValueGroupingComparator(GroupComparator.class);
        conf.setPartitionerClass(FirstPartitioner.class);


        //Start the hadoop job
        JobClient.runJob(conf);
    }

    public static class FirstPartitioner implements Partitioner<DoubleWritable, Text> {

        @Override
        public void configure(JobConf job) {}

        @Override
        public int getPartition(DoubleWritable key, Text value, int numPartitions) {
            double d = (Double.parseDouble(key.toString()));
            int n =(int) d * 100;
            return (int)(n / numPartitions) ;
        }
    }

    public static class GroupComparator extends WritableComparator {
        protected GroupComparator() {
            super(DoubleWritable.class, true);
        }
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            DoubleWritable ip1 = (DoubleWritable) w1;
            DoubleWritable ip2 = (DoubleWritable) w2;
            return ip1.compareTo(ip2);
        }
    }

    public static class KeyComparator extends WritableComparator {
        protected KeyComparator() {
            super(DoubleWritable.class, true);
        }
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            DoubleWritable d1 = (DoubleWritable) w1;
            DoubleWritable d2 = (DoubleWritable) w2;
            int cmp = d1.compareTo(d2);
            return cmp * -1; //reverse
        }
    }

    private Integer readCountFromFile (String filepath, Configuration conf) throws IOException
    {
        String fileName = filepath + "/part-00000";
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
                String[] splits = line.split("=");
                count = Integer.parseInt(splits[1]);
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