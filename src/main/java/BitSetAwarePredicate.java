import com.hazelcast.query.Predicate;

import java.util.Map.Entry;


public class BitSetAwarePredicate implements Predicate{

    private String query;
    public boolean apply(Entry e) {
        // TODO Auto-generated method stub
        Object o = e.getValue();
        if (o instanceof Node){
            Node n = (Node)o;
            if (this.query.equals("is head")){
                if (n.getIncoming().cardinality() == 0)
                    return true;
                else
                    return false;
            }
            else if (this.query.equals("is tail")){
                if (n.getOutgoing().cardinality() == 0)
                    return true;
                else
                    return false;
            }
            else if (this.query.equals("is fork")){
                if (n.getOutgoing().cardinality() > 1)
                    return true;
                else
                    return false;
            }
            else if (this.query.equals("is join")){
                if (n.getIncoming().cardinality() > 1)
                    return true;
                else
                    return false;
            }
        }
        return false;
    }

    public BitSetAwarePredicate(String query){
        this.query = query;
    }

}