package ssfsj.utils;

import java.io.Serializable;
import java.util.*;

public class UpdatePartitionsDocument implements Serializable {
    /**
     * the name of the document
     */
    private String documentName;
    /**
     * all of the key-value pairs by which the document has NOT yet
     * been emitted to the JoinerBolts
     */
    private List<String> sendByThisKVPairs;
    /**
     * all of the key-value pairs for the document
     */
    private List<String> allKVPairs;
    /**
     * boolean value indicating that the document has been already sent to the merger
     */
    private boolean documentAlreadySentToMerger;
    /**
     * set indicating the joiners to which the document was already sent
     */
    private Set<Integer> joinersToWhichDocumentSent;
    /**
     * informing whether the document was received after some AssignerBolt
     * indicated that re-calculation of the partitions should be performed
     */
    private boolean recalculationOfPartitionsInProgress;


    public UpdatePartitionsDocument(String documentName, List<String> allKVPairs){
        this.documentName = documentName;
        this.allKVPairs = allKVPairs;
        this.sendByThisKVPairs = new ArrayList<>();
        this.documentAlreadySentToMerger = false;
        this.joinersToWhichDocumentSent = new HashSet<>();
        this.recalculationOfPartitionsInProgress = false;
    }

    public boolean isRecalculationOfPartitionsInProgress() {
        return recalculationOfPartitionsInProgress;
    }

    public void setRecalculationOfPartitionsInProgress(boolean recalculationOfPartitionsInProgress) {
        this.recalculationOfPartitionsInProgress = recalculationOfPartitionsInProgress;
    }

    public String getDocumentName() {
        return documentName;
    }

    public boolean isDocumentAlreadySentToMerger() {
        return documentAlreadySentToMerger;
    }

    public void setDocumentAlreadySentToMerger(boolean documentAlreadySentToMerger) {
        this.documentAlreadySentToMerger = documentAlreadySentToMerger;
    }

    public void setDocumentName(String documentName) {
        this.documentName = documentName;
    }

    public List<String> getSendByThisKVPairs() {
        return sendByThisKVPairs;
    }

    public void setSendByThisKVPairs(List<String> sendByThisKVPairs) {
        this.sendByThisKVPairs = sendByThisKVPairs;
    }

    public List<String> getAllKVPairs() {
        return allKVPairs;
    }

    public void setAllKVPairs(List<String> allKVPairs) {
        this.allKVPairs = allKVPairs;
    }

    public void addKVPairForSending(String kvPair){
        this.sendByThisKVPairs.add(kvPair);
    }

    public Set<Integer> getJoinersToWhichDocumentSent() {
        return joinersToWhichDocumentSent;
    }

    public void setJoinersToWhichDocumentSent(Set<Integer> joinersToWhichDocumentSent) {
        this.joinersToWhichDocumentSent = joinersToWhichDocumentSent;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UpdatePartitionsDocument that = (UpdatePartitionsDocument) o;
        return Objects.equals(documentName, that.documentName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(documentName);
    }
}
