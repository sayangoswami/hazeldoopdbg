/**
 * DeBruijnGraph.java:
 * Functioanlaity: Single hadoop map-reduce job to calculate kmer-frequency and deBruijngraph-construction
 * Input: Fastq reads
 * Output: Frequency of Kmer followed by the de bruijn graph (adjacency list)
 * Process:
 *  1) Add an extra N to the read (To simplify code)
 *  2) Find out all k+1-mer in a read and return to the mapper
 *  3) Mapper output: Key = <kmer>, value = <1 edge> //k-mer = substring(0, k) of k+1-mer.
 * 													 1 is to count frequency
 * 													 edge = substring(1, k+1) of k+1-mer
 *  4) Reducer output: Key = <kmer>, value = <frequency edge_list>
 *  5) Always remove any kmer with "N"
 * Code Flow:
 *   map --> constructK1mersFromARead --> return to map
 *   reduce -->
 *
 * ToDo:
 *  1) In the edge_list same kmers are placed multiple times.
 *  	Need to discuss, whether keep in that way, or just put unique kmers
 *  2) Json format output (How to deal with that frequency)
 *  3) Better/Simpler/MoreEfficient Algorithm
 */

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

public class DeBruijnGraph {
    public static int k = 31;
    public static int k1 = k+1;
    public static String N = "N";
    public static AdditionalUtilities autil = new AdditionalUtilities();
    public static DeBruijnGraph dbg = new DeBruijnGraph();
    public static String delim = "\t";

    public static class DeBruijnGraphMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
        private Text kmer = new Text();
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            System.out.println("In DeBruijnGraph.map");
            try {
                String line = value.toString();
                if (dbg.isReadLine(line) == true) {
                    String k1mersInARead[] = new String[line.length()-k1+1];
                    k1mersInARead = dbg.constructK1mersFromARead(line);
                    for (int i = 0; i < k1mersInARead.length; i++) {
                        kmer.set(k1mersInARead[i].substring(0, k));
                        if (!kmer.toString().contains(N)) {
                            output.collect(kmer, new Text("1" + delim + k1mersInARead[i].substring(1))); //Key = <kmer>, value = <1 edge>
                        }
                    }
                }
            }
            catch(Exception e) {System.out.println("Exception in DeBruijnGraph.map"); e.printStackTrace();}
        }
    }

    public static class DeBruijnGraphReduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            System.out.println("In DeBruijnGraph.reduce");
            try {
                int frequency = 0;
                String edge_list = "";
                while (values.hasNext()) {
                    String tmpVal =  values.next().toString();
                    StringTokenizer tokenizer = new StringTokenizer(tmpVal, delim);
                    frequency += Integer.parseInt(tokenizer.nextToken()); //First token is the frequency
                    while (tokenizer.hasMoreTokens()) {
                        String tmpToken = tokenizer.nextToken();
                        if (!tmpToken.contains(N)) {
                            edge_list += tmpToken + delim; //Other tokens are added to edge_list
                        }
                    }
                }
                edge_list = autil.deDup(edge_list, delim);
                output.collect(key, new Text(Integer.toString(frequency) + delim + edge_list)); //Key = <kmer>, value = <frequency edge_list>
                //output.collect(key, new Text(edge_list));
            }
            catch(Exception e) {System.out.println("Exception in DeBruijnGraph.reduce"); e.printStackTrace();}
        }
    }

    public String[] constructK1mersFromARead(String read) {
        System.out.println("In DeBruijnGraph.constructK1mersFromARead");
        String tmpRead = read + "N";
        String k1mer[] = new String[tmpRead.length()-k1+1];
        for (int i = 0; i <= tmpRead.length()-k1; i++) {
            //System.out.print("i = " + i + ". ");
            k1mer[i] = tmpRead.substring(i, i+k1);
            //System.out.println(kmer[i]);
        }
        return k1mer;
    }

    public boolean isReadLine(String line) {
        System.out.println("In DeBruijnGraph.isReadLine");
        if (line.matches("[ATGCNP]+")) {
            return true;
        }
        else {
            return false;
        }
    }

    public int run(String ip_path, String op_path) {
        System.out.println("In DeBruijnGraph.run");
        try {
            JobConf conf = new JobConf(DeBruijnGraph.class);
            conf.setJobName("DeBruijnGraph");
            conf.setOutputKeyClass(Text.class);
            conf.setOutputValueClass(Text.class);
            conf.setMapperClass(DeBruijnGraphMap.class);
            conf.setCombinerClass(DeBruijnGraphReduce.class);
            conf.setReducerClass(DeBruijnGraphReduce.class);
            conf.setInputFormat(TextInputFormat.class);
            conf.setOutputFormat(TextOutputFormat.class);
            FileInputFormat.setInputPaths(conf, new Path(ip_path));
            FileOutputFormat.setOutputPath(conf, new Path(op_path));
            JobClient.runJob(conf);
        }
        catch(Exception e) {System.out.println("Exception in DeBruijnGraph.run"); e.printStackTrace();}
        return 1;
    }

    public static void main(String[] args) throws Exception {
        DeBruijnGraph dbg = new DeBruijnGraph();
        File outdir = new File(args[1]);
        if (outdir.exists())
            FileUtils.forceDelete(outdir);
        dbg.run(args[0], args[1]);
    }
}
