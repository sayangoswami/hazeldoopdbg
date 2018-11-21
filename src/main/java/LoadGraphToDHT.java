import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import java.io.File;
import java.io.IOException;

public class LoadGraphToDHT {
    static HazelcastInstance client;
    static IMap<String, Node> map;
    static int i = 0;
    static String delim = "\t";
    public static class GraphToDHTMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            try {
                if (i == 0) {
                    createClientForGraphHT("DBG");
                }
                i++;
                String line = value.toString();
                String[] parts = line.split(delim);

                GraphLoader.load(map, parts);
                //System.out.println("Map Size:" + map.size());
                //System.out.println("Customer with key 1: "+ map.get(1));

                //openDHTClient();
                //String line = value.toString();
                //put(key, Value);
            }
            catch(Exception e) {System.out.println("Exception in GraphToDHT.map"); e.printStackTrace();}
        }
        @Override
        public void close() {
            client.shutdown();
        }
    }
    public static IMap<String, Node> createClientForGraphHT(String graphHTName) {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.addAddress("127.0.0.1:5701");
        client = HazelcastClient.newHazelcastClient(clientConfig);
        map = client.getMap(graphHTName);
        return map;
    }
    public int run(String ip_path, String op_path) {
        System.out.println("In GraphToDHT.run");
        try {
            JobConf conf = new JobConf(DeBruijnGraph.class);
            conf.setJobName("GraphToDHT");
            conf.setOutputKeyClass(Text.class);
            conf.setOutputValueClass(Text.class);
            conf.setMapperClass(GraphToDHTMap.class);
            conf.setNumReduceTasks(0);
            conf.setNumMapTasks(10);
            //conf.setCombinerClass(DeBruijnGraphReduce.class);
            //conf.setReducerClass(DeBruijnGraphReduce.class);
            conf.setInputFormat(TextInputFormat.class);
            conf.setOutputFormat(TextOutputFormat.class);
            FileInputFormat.setInputPaths(conf, new Path(ip_path));
            FileOutputFormat.setOutputPath(conf, new Path(op_path));
            JobClient.runJob(conf);
        }
        catch(Exception e) {System.out.println("Exception in GraphToDHT.run"); e.printStackTrace();}
        return 1;
    }
    public static void main(String[] args) throws Exception {
        LoadGraphToDHT gtd = new LoadGraphToDHT();
        File outdir = new File(args[1]);
        if (outdir.exists())
            FileUtils.forceDelete(outdir);
        gtd.run(args[0], args[1]);
    }
}