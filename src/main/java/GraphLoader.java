import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.BitSet;

public class GraphLoader {
    public static void main(String[] args) {
        if (args.length >= 1){
            String fileName = args[0];
            String mapName;
            if (args.length == 2)
                mapName = args[1];
            else
                mapName = "DBG";
            HazelcastInstance hzc = getNewInstance();
            System.out.printf("Address = %s\n", hzc.getCluster().getLocalMember().getSocketAddress().toString().substring(1));
            IMap<String, Node> dbg = createMap(mapName, hzc);
            bulkLoad(dbg, fileName);
            //updateIncomingEdges(dbg);
            for (Object object : dbg.entrySet()){
                System.out.println(object.toString());
            }

        }
        else {
            System.out.println("Usage: java -jar Load.jar <path to graph file> <optional: name of map>");
        }

    }

    public static HazelcastInstance getNewInstance() {
        //
        Config cfg = new Config();
        Hazelcast.shutdownAll();
        HazelcastInstance hz = Hazelcast.newHazelcastInstance();
        return hz;
    }

    public static void bulkLoad(IMap<String, Node> map, String fileName) {
        //System.out.println("Loading vertices and outgoing edges");
        try{
            BufferedReader reader = new BufferedReader(new FileReader(fileName));
            String line = null;
            while ((line = reader.readLine()) != null){
                //scanner.useDelimiter(",");
                String[] parts = line.split("\t");
                load(map, parts);
            }
            reader.close();
            System.out.println("Done loading outgoing edges. Map size = " + map.size());
        } catch (Exception e) {
            System.out.println("Could not read from file.");
            e.printStackTrace();
        }

        try{
            BufferedReader reader = new BufferedReader(new FileReader(fileName));
            String line = null;
            while ((line = reader.readLine()) != null){
                //scanner.useDelimiter(",");
                String[] parts = line.split("\t");
                updateIncoming(map, parts);
            }
            reader.close();
            System.out.println("Done generating incoming edges edges. Map size = " + map.size());
        } catch (Exception e) {
            System.out.println("Could not read from file.");
            e.printStackTrace();
        }
    }

    public static void load(IMap<String, Node> map, String[] tokens){
        String id = tokens[0];
        String kmer = id;
        int frequency = Integer.parseInt(tokens[1]);
        char[] iedge = {'0','0','0','0'};
        char[] oedge = {'0','0','0','0'};
        for (int i = 2; i < tokens.length; i++){
            String edge = tokens[i];
            switch (edge.charAt(edge.length() - 1)){
                case 'A':
                    oedge[0] = '1';
                    break;
                case 'T':
                    oedge[1] = '1';
                    break;
                case 'G':
                    oedge[2] = '1';
                    break;
                case 'C':
                    oedge[3] = '1';
                    break;
            }
        }
        map.put(id, new Node(kmer, String.copyValueOf(iedge), String.copyValueOf(oedge)));
    }

    public static void updateIncoming(IMap<String, Node> map, String[] tokens) {
        String key = tokens[0];
        int index = -1;
        switch(key.charAt(key.length()-1)){
            case 'A':
                index = 0;
                break;
            case 'T':
                index = 1;
                break;
            case 'G':
                index = 2;
                break;
            case 'C':
                index = 3;
                break;
        }
        Node node = map.get(key);
        BitSet incoming = new BitSet(4);
        //System.out.println("@@@@@@@@@ key = " + key);
        String parent= "A" + node.getKmer().substring(0, node.getK()-1);
        //System.out.println("Trying to find " + parent);
        if ( map.containsKey(parent) ){
            if (map.get(parent).getOutgoing().get(index)){
                incoming.set(0);
                //System.out.println("... Found ");
            }
        }
        parent= "T" + node.getKmer().substring(0, node.getK()-1);
        //System.out.println("Trying to find " + parent);
        if ( map.containsKey(parent) ){
            if (map.get(parent).getOutgoing().get(index)){
                incoming.set(1);
                //System.out.println("... Found ");
            }
        }
        parent= "G" + node.getKmer().substring(0, node.getK()-1);
        //System.out.println("Trying to find " + parent);
        if ( map.containsKey(parent) ){
            if (map.get(parent).getOutgoing().get(index)){
                incoming.set(2);
                //System.out.println("... Found ");
            }
        }
        parent= "C" + node.getKmer().substring(0, node.getK()-1);
        //System.out.println("Trying to find " + parent);
        if ( map.containsKey(parent) ){
            if (map.get(parent).getOutgoing().get(index)){
                incoming.set(3);
                //System.out.println("... Found ");
            }
        }
        node.SetIncoming(incoming);
        map.put(key, node);
    }

    public static IMap<String, Node> createMap(String mapName, HazelcastInstance hzc){
        IMap <String, Node> map = hzc.getMap(mapName);
        return map;
    }
}
