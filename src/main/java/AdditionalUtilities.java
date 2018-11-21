import com.hazelcast.core.IMap;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

public class AdditionalUtilities {
    public static int sumDigits(int number){
        int sum = 0;
        int input = number;
        while (input != 0) {
            int lastdigit = input % 10;
            sum += lastdigit;
            input /= 10;
        }
        return sum;
    }

    public static Node getNode(IMap<String, Node> map, String key) {
        // TODO Auto-generated method stub
        if (key == null) {
            System.out.println("Returning null.");
            return null;
        }
        else {
            //System.out.printf("getNode() returning value = %s\n", map.get(key).getKmer());
            return map.get(key);
        }
    }

    public static void updateNode(IMap<String, Node> map, String key, Node node){
        map.replace(key, node);
    }

    public static Set union(Set<String> A, Set<String> B){
        Set<String> tmp = A;
        tmp.addAll(B);
        return tmp;
    }

    public static String deDup(String s, String delim) {
        return new LinkedHashSet<String>(Arrays.asList(s.split(delim))).toString().replaceAll("(^\\[|\\]$)", "").replace(", ", delim);
    }
    public String mergeStringArray(String[] strarr) {
        String str = "";
        for (int i = 0; i < strarr.length; i++) {
            str += strarr[i] + "\t";
        }
        return str;
    }
    public String changeCharacters(String str) {
        int strLen = str.length();
        char[] changedStr = str.toCharArray();
        for (int i = 0; i < strLen; i++) {
            if (str.charAt(i) == 'A' || str.charAt(i) == 'T' || str.charAt(i) == 'G' || str.charAt(i) == 'C') {
                changedStr[i] = (char)((int)str.charAt(i)+1);
                //changedStr[i] = (char)((int)str.charAt(i)); //For csv format gephi graph visualization
            }
        }
        return new String(changedStr);
    }

    public static void printAll(IMap<String, Node> map){
        for (Object o : map.entrySet()){
            System.out.println(o.toString());
        }
    }
}
