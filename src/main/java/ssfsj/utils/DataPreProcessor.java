package ssfsj.utils;

import java.util.*;

public class DataPreProcessor {
    /**
     * integer representing the number of documents used for partitioning
     */
    int numberOfDocuments;
    /**
     * integer representing the number of partitions
     * that should be created
     */
    int k;
    /**
     * map representing the data
     * key->documentId
     * value-> Set of key-value pairs
     */
    Map<String, HashSet<String>> data;
    /**
     * List representing statistics for a key
     */
    List<KeyStatistics> keyStatistics;
    /**
     * map representing the number of documents in which the key-value pairs appear
     */
    HashMap<String,Integer> numberOfDocumentsInWhichKeyValuePairAppears;


    public DataPreProcessor(int k){
        this.k = k;
        this.numberOfDocumentsInWhichKeyValuePairAppears = new HashMap<>();
        this.keyStatistics = new ArrayList<>();
    }

    public int getK(){
        return this.k;
    }

    public int getNumberOfDocuments(){
        return this.numberOfDocuments;
    }

    public List<KeyStatistics> getKeyStatistics(){
        return keyStatistics;
    }

    public void setData(Map<String,HashSet<String>> data){
        this.data = data;
        this.numberOfDocuments=this.data.keySet().size();
    }

    public ArrayList<String> modifyDataIfIncorrectNumberOfPartitions(){
        ArrayList<String> joinedKVPairs = new ArrayList<>();
        ArrayList<String> keysThatShouldBeJoined = new ArrayList<>();

        int numberOfPartitionsAtBeginning = 0;
        for(int i=0;i<keyStatistics.size();i++){
            KeyStatistics keyStatistic = keyStatistics.get(i);
            if(i==0){
                //if with the first value the partitions can be satisfied immediately break
                if(keyStatistic.values.size() >=this.k){
                    break;
                }
            }
            if(numberOfPartitionsAtBeginning < this.k){
                joinedKVPairs.add(keyStatistic.key);
                keysThatShouldBeJoined.add(keyStatistic.key);
//                System.out.println("Key: " + keyStatistic.key + ", num of distinct values: " + keyStatistic.values.size());
                numberOfPartitionsAtBeginning += keyStatistic.values.size();
            }else{
                break;
            }
        }

        if(keysThatShouldBeJoined.size() != 0){
            System.out.println("Now we can create: " + numberOfPartitionsAtBeginning + " partitions");
            System.out.println("Some keys need to be joined!!!");
            System.out.println("We should join the keys: ");
            for(String key : keysThatShouldBeJoined){
                System.out.println("Key -> " + key);
            }
            System.out.println("-------------------------");
            System.out.println();
            /**
             * documents that should be updated
             */
            HashMap<String, HashSet<String>> dataThatShouldBeUpdated = new HashMap<>();
            for(String documentId : this.data.keySet()){
                HashSet<String> kvPairsForDocument = new HashSet<>(this.data.get(documentId));

                HashSet<String> kvPairsThatShouldBeRemoved = new HashSet<>();
                ArrayList<String> valuesThatShouldBeAdded =new ArrayList<>();
                for(String key : keysThatShouldBeJoined){
                    for(String kvPair : kvPairsForDocument){
                        String splitKvPair[] = kvPair.split("-");
                        if(splitKvPair[0].trim().equals(key)){

                            /**
                             * add the value for the first key
                             */
                            StringBuilder sbVal = new StringBuilder(splitKvPair[1].trim());
                            valuesThatShouldBeAdded.add(sbVal.toString());
                            /**
                             * say that this key-value pairs should be removed
                             */
                            StringBuilder sb = new StringBuilder(kvPair);
                            kvPairsThatShouldBeRemoved.add(sb.toString());
                        }
                    }

                }
                /**
                 * perform the removal only if the documents contain the keys that should be joined
                 */

                if(kvPairsThatShouldBeRemoved.size() > 0) {
                    /**
                     * create the new key and value
                     */
                    //join the keys and the values with the special character "|"
                    String newKey = String.join("|", keysThatShouldBeJoined);
                    String newValue = String.join("|", valuesThatShouldBeAdded);

                    /**
                     * remove the old key and value for the document
                     */
                    for (String kvPairThatShouldBeRemoved : kvPairsThatShouldBeRemoved) {
                        kvPairsForDocument.remove(kvPairThatShouldBeRemoved);
                    }

                    kvPairsForDocument.add(newKey+"-"+newValue);
                    /**
                     * update the data
                     */
                    dataThatShouldBeUpdated.put(documentId, kvPairsForDocument);
                }
            }

            /**
             * update the documents with the actual key-value pairs
             */
            for(String documentId : dataThatShouldBeUpdated.keySet()){
                this.data.put(documentId, dataThatShouldBeUpdated.get(documentId));
            }
        }

        return joinedKVPairs;
    }

    public void calculateDataStatistics(){
        /**
         * this map is used in order to get all the statistics for the key
         */
        HashMap<String,KeyStatistics> mapOfKeyStatistics = new HashMap<>();

        /**
         * iterate over the documents
         */
        for(String documentId : this.data.keySet()){
            /**
             * iterate over the key-value pairs of the document
             */
            for(String keyValuePair : this.data.get(documentId)){
                String splitKvPair[] = keyValuePair.split("-");
                /**
                 * get the statistics for the key if they exist
                 */
                KeyStatistics keyStatistic = mapOfKeyStatistics.get(splitKvPair[0].trim());
                /**
                 * if they do not exist create a new object
                 */
                if(keyStatistic==null){
                    keyStatistic = new KeyStatistics(splitKvPair[0].trim());
                }

                /**
                 * add the document and the current value for the key in the keyStatistics object
                 */
                keyStatistic.documentsInWhichKeyAppears.add(documentId);
                keyStatistic.values.add(splitKvPair[1].trim());

                /**
                 * store the updated keyStatistics object in the map
                 */
                mapOfKeyStatistics.put(splitKvPair[0].trim(),keyStatistic);
            }
        }
        /**
         * populate the array list with the key-statistics object
         */
        for(String key : mapOfKeyStatistics.keySet()){
            KeyStatistics keyStatistics = mapOfKeyStatistics.get(key);

            this.keyStatistics.add(keyStatistics);
        }

        /**
         * sort the array list in descending order based on the number of documents in which the key-appears
         * if two keys appear in the same documents then sort them based on the distinct values that they have
         */

        Collections.sort(this.keyStatistics);
    }

    public Map<String,HashSet<String>> getData(){
        return this.data;
    }
}
