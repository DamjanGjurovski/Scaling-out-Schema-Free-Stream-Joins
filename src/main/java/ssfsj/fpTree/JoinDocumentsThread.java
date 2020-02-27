package ssfsj.fpTree;

import ssfsj.utils.KeyValuePair;

import java.util.*;

public class JoinDocumentsThread extends Thread {

    private int startNumber;
    private int endNumber;
    private Map<String, Set<String>> joinableDocuments;
    private TreeWithMap fpTree;
    private Map<String, List<KeyValuePair>> allDocuments;
    //integer representing the number of keys that were present in every document
    private int numOfKeysInEveryDoc;

    public JoinDocumentsThread(int startNumber, int endNumber, TreeWithMap fpTree, Map<String,List<KeyValuePair>> allDocuments, int numOfKeysInEveryDoc){
        this.startNumber = startNumber;
        this.endNumber = endNumber;
        this.fpTree = fpTree;
        this.joinableDocuments = new HashMap<>();
        this.allDocuments = allDocuments;
        this.numOfKeysInEveryDoc = numOfKeysInEveryDoc;
    }

    @Override
    public void run() {
        //needed so that every thread will operate with a pre-specified number of documents
        int i=0;
        for(String documentId : this.allDocuments.keySet()){
            if(i>=startNumber && i<endNumber){
                //get the key-value pairs in sorted order for the current document
                List<KeyValuePair> kvPairs = this.allDocuments.get(documentId);
                //get all of the keys for the document
                ArrayList<String> keys = new ArrayList<>();
                for (KeyValuePair kvP : kvPairs) {
                    keys.add(kvP.getKey());
                }

                /**
                 * search by utilize the fact that there are keys that appears in all of the documents
                 */
                Set<String> allJoinableDocumentsForGivenDocument = fpTree.searchingForJoinableDocumentsByUsingKVPairsOfDocument(documentId, kvPairs, keys, new HashSet<>(), this.numOfKeysInEveryDoc);

                this.joinableDocuments.put(documentId, allJoinableDocumentsForGivenDocument);
            }
            i++;
        }

    }

    public Map<String, Set<String>> getJoinableDocuments() {
        return joinableDocuments;
    }

    public void setJoinableDocuments(Map<String, Set<String>> joinableDocuments) {
        this.joinableDocuments = joinableDocuments;
    }
}
