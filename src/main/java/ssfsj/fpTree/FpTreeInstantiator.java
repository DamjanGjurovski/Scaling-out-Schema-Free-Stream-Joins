package ssfsj.fpTree;

import ssfsj.utils.KeyValuePair;
import ssfsj.utils.PostingList;
import ssfsj.utils.sorting.QuickSortModifiedAlgorithm;
import ssfsj.utils.sorting.SortingObject;

import java.io.Serializable;
import java.util.*;

public class FpTreeInstantiator implements Serializable {
    /**
     * FPTree representing all of the documents as branches of key-value pairs in the tree.
     */
    private TreeWithMap fpTree;
    /**
     * map of key-value pairs per document where the key-value pairs are sorted in
     * fixed order.
     */
    private Map<String, List<KeyValuePair>> kvPairsPerDocumentSorted;
    /**
     *  integer representing the number of keys that are present in all of the documents
     */
    private int numOfKeysInEveryDoc;

    public FpTreeInstantiator(){
        this.fpTree = new TreeWithMap();
        this.kvPairsPerDocumentSorted = new HashMap<>();
        this.numOfKeysInEveryDoc = 0;
    }

    public void createFpTree(HashMap<String, HashSet<KeyValuePair>> kvPairsForDocuments){
        //remove the existing tree
        this.fpTree = new TreeWithMap();
        //remove the existing documents
        this.kvPairsPerDocumentSorted = new HashMap<>();
        //remove the existing number of key-value pairs
        this.numOfKeysInEveryDoc = 0;

        //index of the key-value pairs
        HashMap<String, PostingList> indexForKVPair = createIndexForKVPairs(kvPairsForDocuments);
        /**
         * sort the key-value pairs of the documents
         * -> the keys are sorted based on their occurrence in the documents in descending order
         *    meaning that the keys that appear in the most documents will be first in the list
         */
        /**
         * fixed ordering of the keys
         */
        //create an object of the sorting algorithm
        QuickSortModifiedAlgorithm quickSortAlgorithm = new QuickSortModifiedAlgorithm();
        //map for fixed sorting order of the keys
        HashMap<String, Integer> keysStrength = new HashMap<>();
        int somePosition = 0;
        ArrayList<SortingObject> sortinObjectsList = new ArrayList<>();
        //first create a fixed sorting order of the keys that will be used for every key-value pair
        for(String key : indexForKVPair.keySet()){
            PostingList postingList = indexForKVPair.get(key);
            SortingObject sortingObject = new SortingObject();
            sortingObject.value = postingList.numOfOccurrencesOfKey;
            sortingObject.position = somePosition;
            sortingObject.key = key;
            sortingObject.numberOfDistinctValues = postingList.getDocumentsPerValue().keySet().size();

            //if the key appears in all of the documents increment numOfKeysInEveryDoc
            if(sortingObject.value==kvPairsForDocuments.size()){
                this.numOfKeysInEveryDoc++;
            }
            sortinObjectsList.add(sortingObject);
            somePosition+=1;

        }
        //sort the keys in ascending order
        quickSortAlgorithm.sort(sortinObjectsList);
        //assign the sorting order of every key by taking the last key and adding the largest position and so on..
        for(int i=sortinObjectsList.size()-1;i>=0;i--){
            SortingObject sortingObject = sortinObjectsList.get(i);
            keysStrength.put(sortingObject.key, i);
        }

        /**
         * sort the key-value pairs of the documents
         */
        //iterate over the documents
        for(String documentId : kvPairsForDocuments.keySet()){
            //create a list of sorting objects
            ArrayList<SortingObject> sortingObjects = new ArrayList<>();
            int positionOfObject = 0;
            //iterate over the key-value pairs of the document
            for(KeyValuePair keyValuePair : kvPairsForDocuments.get(documentId)){
                //create a sorting object
                SortingObject sortingObject = new SortingObject();
                //set the value which is the number of documents in which the key appears
                sortingObject.value = keysStrength.get(keyValuePair.getKey());
                //set the position
                sortingObject.position = positionOfObject;
                //set the key-value pair
                sortingObject.keyValuePair = keyValuePair;

                //add the sorting object to the list
                sortingObjects.add(positionOfObject, sortingObject);
                //increment the position
                positionOfObject+=1;
            }
            //perform sorting of the key-value pairs based on the number of documents in which the key appears
            //in ascending order
            quickSortAlgorithm.sort(sortingObjects);
            ArrayList<KeyValuePair> finalKVPairs = new ArrayList<>();
            //assign the key-value pairs in descending order
            for(int i=sortingObjects.size()-1;i>=0;i--){
                SortingObject sortingObject = sortingObjects.get(i);
                KeyValuePair kvPair = sortingObject.keyValuePair;
                kvPair.numOfDocsForKey = sortingObject.value;

                finalKVPairs.add(kvPair);
            }
            //populate the final map
            this.kvPairsPerDocumentSorted.put(documentId,finalKVPairs);
        }

        //call the method for creating the tree
        this.assignDocumentsToTree();
    }

    /**
     * Method for creating an index of the following form:
     *   key1
     *      value1 -> doc1,doc2,...
     *      value2 -> doc3,doc7,...
     *   key2
     *      value3->doc1,..
     *      value4->doc3,....
     * @param kvPairsForDocuments
     * @return
     */
    private HashMap<String, PostingList> createIndexForKVPairs(HashMap<String, HashSet<KeyValuePair>> kvPairsForDocuments){
        //the index of the key-value pairs
        HashMap<String, PostingList> indexForKVPair = new HashMap<>();
        //iterate over all of the documents
        for(String documentId : kvPairsForDocuments.keySet()){
            //for every document get the key-value pairs
            HashSet<KeyValuePair> kvPairsForDoc = kvPairsForDocuments.get(documentId);
            for(KeyValuePair kvPair : kvPairsForDoc){
                //for every key-value pair check if the key already exists in the index
                PostingList postingList = indexForKVPair.get(kvPair.getKey());
                if(postingList==null){//if the key doesn't exist create a new Posting List
                    postingList = new PostingList(kvPair.getKey());
                }
                //update the posting list with the documents for the value
                postingList.addDocumentsForValue(kvPair.getValue(), documentId);
                //store the information in the index
                indexForKVPair.put(kvPair.getKey(), postingList);
            }
        }
        return indexForKVPair;
    }

    /**
     * Method used for creating the FPTree by assigning the key-value pairs of the documents
     * as branches of the tree.
     */
    private void assignDocumentsToTree(){
        for(String document : this.kvPairsPerDocumentSorted.keySet()){
            //add the key-value pairs from the document as nodes in the tree
            this.fpTree.addNewNodesFromDocument(this.kvPairsPerDocumentSorted.get(document),document);
        }
        //update the node depths
        this.fpTree.updateNodeDepthUsingNodeIds(this.fpTree.getRoot());
    }

    /**
     * Method for finding the joinable documents in a parallel manner. Multiple
     * threads are started for the FPTree in order to iterate over it faster.
     * @return
     */
    public Map<String,Set<String>> findJoinableDocumentsThroughThreads(){
        //since the search for every document is independent of the others
        //the search through the FP-tree can be further improved with multithreading
        int numOfThreads = 1;
        int numOfDocuments = kvPairsPerDocumentSorted.keySet().size();
        ArrayList<JoinDocumentsThread> threadPool = new ArrayList<>();
        //creating a start and end index need so that every thread will operate on a pre-specified set of documents
        int startIndex = 0;
        int endIndex = (numOfDocuments/numOfThreads);
        for(int i=0;i<numOfThreads;i++){
            //creating the threads
            JoinDocumentsThread joinDocumentsThread = new JoinDocumentsThread(startIndex,endIndex,this.fpTree,this.kvPairsPerDocumentSorted,this.numOfKeysInEveryDoc);
            threadPool.add(joinDocumentsThread);
            //updating the start and end index
            startIndex = endIndex;
            endIndex = (numOfDocuments/numOfThreads)*(i+2);
            if(i+2==numOfThreads)
                endIndex = numOfDocuments;

        }

        /**
         * start all the threads
         */
        for(JoinDocumentsThread joinDocumentsThread : threadPool){
            joinDocumentsThread.start();
        }
        /**
         * wait for all the threads to finish before continuing
         */
        for(JoinDocumentsThread joinDocumentsThread : threadPool){
            try {
                joinDocumentsThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        //get all the joinable documents from every thread in one map
        HashMap<String,Set<String>> joinableDocs = new HashMap<>();
        for(JoinDocumentsThread joinDocumentsThread :threadPool){
            joinableDocs.putAll(joinDocumentsThread.getJoinableDocuments());
        }

        return joinableDocs;
    }

    public TreeWithMap getFpTree(){
        return this.fpTree;
    }

    public int getNumOfKeysInEveryDoc(){
        return this.numOfKeysInEveryDoc;
    }

}
