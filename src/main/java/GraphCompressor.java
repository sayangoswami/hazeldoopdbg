import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.query.Predicate;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Set;

public class GraphCompressor {

    static int k;

    public static void main(String[] args) {
        if (args.length >= 2){
            String address = args[0];
            k = Integer.parseInt(args[1]);
            String mapName;
            if (args.length == 3)
                mapName = args[2];
            else
                mapName = "DBG";

            IMap<String, Node> map = initClient(address, mapName);
            System.out.println("Compression : Round 1");
            compress(map);

            //System.out.println("Compression : Round 2");
            //compress(map);

            //String seed = getSeed(map);
			
			
			/*for (Object o : map.entrySet()){
				System.out.println(o.toString());
			}*/
        }
        else {
            System.out.println("Usage: java -jar Compress.jar <address> <k> <optional: name of map>");
        }
    }

    public static void compress(IMap<String, Node> map) {
        Set<String> keys = getSeeds(map);
        for (String key : keys){
            Node seedNode = AdditionalUtilities.getNode(map, key);
            seedNode.setSeed(true);
            AdditionalUtilities.updateNode(map, key, seedNode);
            System.out.printf("Seed = %s\n", key);
            //traverseForward(map, key); // TODO uncomment
        }
    }

    public static Set<String> getSeeds(IMap<String, Node> map) {
        /**
         * Seeds are those which are either
         * 1. unambiguous heads, or
         * 2. the very first nodes after forks or
         * 3. joins
         */
        Predicate getHeads = new BitSetAwarePredicate("is head");
        Predicate getJoins = new BitSetAwarePredicate("is join");
        Predicate getForks = new BitSetAwarePredicate("is fork");

        Set<String> heads = map.keySet(getHeads);
        System.out.printf("Number of heads = %d\n", heads.size());
        Set<String> joins = map.keySet(getJoins);
        System.out.printf("Number of joins = %d\n", joins.size());
        Set<String> forks = map.keySet(getForks);
        System.out.printf("Number of forks = %d\n", forks.size());

        Set<String> seeds = getSiblings(map, forks);
        seeds.addAll(heads);
        seeds.addAll(joins);

        return seeds;
    }

    private static Set<String> getSiblings(IMap<String, Node> map, Set<String> forks) {
        Set<String> seeds = new HashSet<String>();
        for (String key : forks){
            Node n = map.get(key);
            BitSet b = n.getOutgoing();
            if (b.get(0)){
                seeds.add(key.substring(1).concat("A"));
            }
            if (b.get(1)){
                seeds.add(key.substring(1).concat("T"));
            }
            if (b.get(2)){
                seeds.add(key.substring(1).concat("G"));
            }
            if (b.get(3)){
                seeds.add(key.substring(1).concat("C"));
            }
        }
        return seeds;
    }

    public static IMap<String, Node> initClient(String address, String mapName) {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.addAddress(address);
        HazelcastInstance hz = HazelcastClient.newHazelcastClient(clientConfig);
        IMap<String, Node> map = hz.getMap(mapName);
        return map;
    }

    public static void traverseForward(IMap<String, Node> map, String seed){
        Node seedNode = AdditionalUtilities.getNode(map, seed);
        String contig = seedNode.getKmer();
        Node nextNode = seedNode;
        File contigFile = new File("contigs.fa");
        while (nextNode.getOutgoing().cardinality() == 1 && nextNode.getIncoming().cardinality() <= 1){
            String[] nextKey = seedNode.getNextNodes();
            nextNode = AdditionalUtilities.getNode(map, nextKey[0]);
            if (nextNode == null){
                System.out.println("Next node was null.");
                break;
            }
            else if (nextNode.isSeed()){
                System.out.println("Reached another seed.. Stopping");
                break;

            }
            else {
                contig = contig + nextNode.getKmer().substring(k-1);
                seedNode.setOutgoing(nextNode.getOutgoing());
                map.remove(nextKey);
                seedNode.setKmer(contig);
            }
        }
        System.out.printf("Final Map size = %d\n", map.size());
        seedNode.setSeed(false);
        seedNode.setKmer(contig);
        map.replace(seed, seedNode);
        //System.out.println(contig);
        try {
            FileUtils.writeStringToFile(contigFile, contig.length() + "\n" + contig + "\n", true);
        } catch (IOException e) {
            System.out.println("Could not write contig to file..");
        }
        System.out.println("Size of contig from this seed = " + contig.length());
    }
}