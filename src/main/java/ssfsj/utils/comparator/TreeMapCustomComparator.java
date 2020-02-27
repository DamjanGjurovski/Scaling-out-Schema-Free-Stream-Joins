package ssfsj.utils.comparator;

import java.util.Comparator;
import java.util.HashSet;

/**
 * This comparator sorts in ascending order. For reversed use .reversed()
 */
public class TreeMapCustomComparator implements Comparator<HashSet<String>> {
    @Override
    public int compare(HashSet<String> strings, HashSet<String> t1) {
        if(strings.size() < t1.size()){
            return -1;
        }else if(strings.size() > t1.size()){
            return 1;
        }else{
            if(strings.containsAll(t1)){
                return 0;
            }
            return -1;
        }

    }
}
