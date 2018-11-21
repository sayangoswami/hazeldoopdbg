import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

public class Driver {
    public static void main(String[] args) {
        try {
            Config config = new XmlConfigBuilder(args[0]).build();
            HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);
            IMap<String, Node> map = GraphLoader.createMap("DBG", hz);
        } catch (Exception e) {
            System.out.println("Config file not found.");
        }
    }
}
