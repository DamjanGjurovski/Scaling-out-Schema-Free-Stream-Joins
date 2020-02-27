package ssfsj.utils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Objects;

public class PostingList {
    private String key;
    private HashMap<String, HashSet<String>> documentsPerValue;
    public int numOfOccurrencesOfKey;

    public PostingList(String key){
        this.key = key;
        this.numOfOccurrencesOfKey = 0;
        this.documentsPerValue = new HashMap<>();
    }

    public void addDocumentsForValue(String value, String documentId){
        HashSet<String> existingDocuments = documentsPerValue.get(value);
        if(existingDocuments==null){
            existingDocuments = new HashSet<>();
        }
        existingDocuments.add(documentId);

        this.documentsPerValue.put(value, existingDocuments);
        this.numOfOccurrencesOfKey+=1;
    }

    public void printSizeOfValues(){
        int actualSize = 0;
        for(String value : this.documentsPerValue.keySet()){
            actualSize+=this.documentsPerValue.get(value).size();
        }
        System.out.println("Actual size of " + this.key +": " + actualSize);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PostingList that = (PostingList) o;
        return Objects.equals(key, that.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key);
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public HashMap<String, HashSet<String>> getDocumentsPerValue() {
        return documentsPerValue;
    }

    public void setDocumentsPerValue(HashMap<String, HashSet<String>> documentsPerValue) {
        this.documentsPerValue = documentsPerValue;
    }
}
