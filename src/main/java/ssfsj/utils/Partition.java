package ssfsj.utils;

import java.io.Serializable;
import java.util.HashSet;

public class Partition implements Serializable {
    public HashSet<String> elementsForPartition;
    public HashSet<String> documentsInWhichAllElementsAppear;
    public HashSet<String> allKVPairs;
    public int numberOfDocumentsForPartition;

    public Partition(){
        this.elementsForPartition = new HashSet<>();
        this.documentsInWhichAllElementsAppear = new HashSet<>();
        this.allKVPairs = new HashSet<>();
        this.numberOfDocumentsForPartition = 0;
    }

    public Partition(HashSet<String> elementsForPartition, HashSet<String> documentsInWhichAllElementsAppear){
        this.elementsForPartition = elementsForPartition;
        this.documentsInWhichAllElementsAppear = documentsInWhichAllElementsAppear;
        this.allKVPairs = new HashSet<>();
        this.numberOfDocumentsForPartition = 0;
    }

    public void addAllKvPairs(HashSet<String> kvPairs){
        this.allKVPairs.addAll(kvPairs);
    }

    public void addDocument(String document){
        this.documentsInWhichAllElementsAppear.add(document);
    }

    public void addAllDocuments(HashSet<String> documents){
        this.documentsInWhichAllElementsAppear.addAll(documents);
    }

    public void addElement(String element){
        this.elementsForPartition.add(element);
    }

    public void addAllElements(HashSet<String> elements){
        this.elementsForPartition.addAll(elements);
    }

    public int loadForPartition(){
        return this.documentsInWhichAllElementsAppear.size();
    }

    public int getNumberOfDocumentsForPartition() {
        return numberOfDocumentsForPartition;
    }

    public void setNumberOfDocumentsForPartition(int numberOfDocumentsForPartition) {
        this.numberOfDocumentsForPartition = numberOfDocumentsForPartition;
    }
}
