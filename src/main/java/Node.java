import java.io.Serializable;
import java.util.BitSet;

public class Node implements Serializable {
    private String kmer;
    private BitSet incoming;
    private BitSet outgoing;
    private boolean isSeed;
    private int k;

    public Node(String kmer, String incoming, String outgoing){
        //System.out.printf("%s\t%s\t%s\n", kmer, incoming, outgoing);
        this.kmer = kmer;
        this.k = kmer.length();
        this.incoming = new BitSet(4);
        this.outgoing = new BitSet(4);
        for (int i = 0; i < 4; i++){
            if (incoming.charAt(i) == '1')
                this.incoming.set(i);
        }
        for (int i = 0; i < 4; i++){
            if (outgoing.charAt(i) == '1')
                this.outgoing.set(i);
        }
    }

	/*public Node(String kmer, BitSet incoming, String outgoing){
		//System.out.printf("%s\t%s\t%s\n", kmer, incoming, outgoing);
		this.kmer = kmer;
		this.k = kmer.length();
		//this.incoming = new BitSet(4);
		this.outgoing = new BitSet(4);
		for (int i = 0; i < 4; i++){
			if (outgoing.charAt(i) == '1')
				this.outgoing.set(i);
		}
		this.incoming = incoming;
	}*/

    public int getK(){
        return this.k;
    }

    public String getKmer() {
        return kmer;
    }

    public void setKmer(String kmer) {
        this.kmer = kmer;
    }

    public BitSet getIncoming() {
        return incoming;
    }

    public void setIncoming(String incoming) {
        for (int i = 0; i < 4; i++){
            if (incoming.charAt(i) == '1')
                this.incoming.set(i);
        }
    }

    public void SetIncoming(BitSet incoming){
        this.incoming = incoming;
    }

    public BitSet getOutgoing() {
        return outgoing;
    }

    public void setOutgoing(String outgoing) {
        for (int i = 0; i < 4; i++){
            if (outgoing.charAt(i) == '1')
                this.outgoing.set(i);
        }
    }

    public void setOutgoing(BitSet outgoing){
        this.outgoing = outgoing;
    }

    public boolean isSeed() {
        return isSeed;
    }

    public void setSeed(boolean isSeed) {
        this.isSeed = isSeed;
    }

    @Override
    public String toString() {
        return "Node [kmer=" + kmer + ", incoming=" + incoming + ", outgoing=" + outgoing + ", isSeed=" + isSeed + "]";
    }

    public String[] getNextNodes() {
        String[] outgoing = new String[this.outgoing.cardinality()];
        int index = 0;
        if (this.outgoing.get(0)){
            StringBuilder sb = new StringBuilder();
            outgoing[index] = sb.append(kmer.substring(this.kmer.length() - this.k + 1)).append("A").toString();
            index++;
        }
        if (this.outgoing.get(1)){
            StringBuilder sb = new StringBuilder();
            outgoing[index] = sb.append(kmer.substring(this.kmer.length() - this.k + 1)).append("T").toString();
            index++;
        }
        if (this.outgoing.get(2)){
            StringBuilder sb = new StringBuilder();
            outgoing[index] = sb.append(kmer.substring(this.kmer.length() - this.k + 1)).append("G").toString();
            index++;
        }
        if (this.outgoing.get(3)){
            StringBuilder sb = new StringBuilder();
            outgoing[index] = sb.append(kmer.substring(this.kmer.length() - this.k + 1)).append("C").toString();
            index++;
        }
        return outgoing;
    }

    public String[] getPreviousNodes(){
        String[] incoming = new String[this.incoming.cardinality()];
        int index = 0;
        if (this.incoming.get(0)){
            StringBuilder sb = new StringBuilder();
            incoming[index] = sb.append("A").append(this.kmer.substring(0,k-1)).toString();
            index++;
        }
        if (this.incoming.get(1)){
            StringBuilder sb = new StringBuilder();
            incoming[index] = sb.append("T").append(this.kmer.substring(0,k-1)).toString();
            index++;
        }
        if (this.incoming.get(2)){
            StringBuilder sb = new StringBuilder();
            incoming[index] = sb.append("G").append(this.kmer.substring(0,k-1)).toString();
            index++;
        }
        if (this.incoming.get(3)){
            StringBuilder sb = new StringBuilder();
            incoming[index] = sb.append("C").append(this.kmer.substring(0,k-1)).toString();
            index++;
        }
        return incoming;
    }
}
