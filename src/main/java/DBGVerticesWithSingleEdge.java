import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;


public class DBGVerticesWithSingleEdge {
    public static String keySeparator = "\t";
    public static String valSeparator = ",";
    public static class DBGVerticesWithSingleEdgeMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
        AdditionalUtilities autils = new AdditionalUtilities();
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            System.out.println("In DBGVerticesWithSingleEdge.map");
            try {
                String line = value.toString();
                String[] parts = line.split(keySeparator); //separate key and value
                String vertexWithSingleEdge = parts[0];
                String edge = "";
                String[] neighbors = parts[1].split(valSeparator);
                if (neighbors.length == 2) {
                    edge = neighbors[1];
                    output.collect(new Text(vertexWithSingleEdge), new Text(edge));
                }
            }
            catch(Exception e) {System.out.println("Exception in DBGVerticesWithSingleEdge.map"); e.printStackTrace();}
        }
    }
    public int run(String ip_path, String op_path) {
        System.out.println("In DBGVerticesWithSingleEdge.run");
        try {
            JobConf conf = new JobConf(DeBruijnGraph.class);
            conf.setJobName("DeBruijnGraphIndexer");
            conf.setOutputKeyClass(Text.class);
            conf.setOutputValueClass(Text.class);
            conf.setMapperClass(DBGVerticesWithSingleEdgeMap.class);
            conf.setNumReduceTasks(0);
            //conf.setCombinerClass(DeBruijnGraphReduce.class);
            //conf.setReducerClass(DeBruijnGraphReduce.class);
            conf.setInputFormat(TextInputFormat.class);
            conf.setOutputFormat(TextOutputFormat.class);
            FileInputFormat.setInputPaths(conf, new Path(ip_path));
            FileOutputFormat.setOutputPath(conf, new Path(op_path));
            JobClient.runJob(conf);
        }
        catch(Exception e) {System.out.println("Exception in DBGVerticesWithSingleEdge.run"); e.printStackTrace();}
        return 1;
    }
    public static void main(String[] args) throws Exception {
        DBGVerticesWithSingleEdge dbgvws = new DBGVerticesWithSingleEdge();
        dbgvws.run(args[0], args[1]);
    }
}