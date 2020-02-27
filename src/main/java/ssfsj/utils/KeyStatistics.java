package ssfsj.utils;

import java.util.HashSet;
import java.util.Set;

public class KeyStatistics implements Comparable<KeyStatistics>{
    String key;
    Set<String> values;
    Set<String> documentsInWhichKeyAppears;

    public KeyStatistics(String key){
        this.key = key;
        this.values = new HashSet<>();
        this.documentsInWhichKeyAppears = new HashSet<>();
    }

    public void addDocumentInWhichKeyAppears(String document){
        this.documentsInWhichKeyAppears.add(document);
    }

    public void addValue(String value){
        this.values.add(value);
    }

    public String getKey() {
        return key;
    }

    public Set<String> getValues() {
        return values;
    }

    /**
     * sort the keys based on the number of documents in which they appear in descending order.
     * If they appear in the same number of documents sort them based on the number of distinct values
     * that they have again in descending order.
     * @param o
     * @return
     */
    @Override
    public int compareTo(KeyStatistics o) {
        if(this.documentsInWhichKeyAppears.size()==o.documentsInWhichKeyAppears.size()){
            return Integer.compare(this.values.size(),o.values.size());
        }
        return Integer.compare(o.documentsInWhichKeyAppears.size(),this.documentsInWhichKeyAppears.size());
    }
}