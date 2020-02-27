package ssfsj.bolts.assigner;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import ssfsj.utils.CommunicationMessages;
import ssfsj.utils.UpdatePartitionsDocument;

import java.util.*;

public class AssignerBolt extends BaseBasicBolt {
    /**
     * logger for AssignerBolt
     */
    private static final Logger LOG = LogManager.getLogger(AssignerBolt.class);

    /**
     * constant value indicating that once we see a kv-pair in X documents we should
     * inform the merger that it needs to update the partitions with that document
     */
    private static final int whenToSentMsgToMergerForUpdatingPartitions = 3;

    /**
     * map for storing to which JoinerBolt the key-value pair should be sent
     */
    HashMap<String, HashSet<Integer>> joinerForKVPair;

    /**
     * boolean value that indicates whether partitions have already been created or not
     */
    boolean partitionsCreated;
    /**
     * list of the JoinerBolt instance ids
     */
    List<Integer> joinerBoltIds;

    /**
     * map used for storing all of the documents that have been received in between the creation of the partitions and assigning the partitions to joiners
     */
    HashMap<String, UpdatePartitionsDocument> batchOfDocumentsReceivedInBetween;

    /**
     * map used for counting in how many documents an unseen key-value pair has appeared. If this number
     * is higher than a pre-specified threshold than the document containing that kv-pair will be sent to the
     * MergerBolt for updating the partitions. If not the instance of the AssignerBolt will only update its local partitions
     * by assigning every joiner to be responsible for that document.
     */
    Map<String, Integer> numberOfTimesKVPairReceived;

    /**
     * ID of the AssignerBolt instance
     */
    int assignerBoltId;

    /**
     * counting the number of documents received from the Spout.
     */
    int numOfDocumentsReceived;

    /**
     * maximal number of documents assigned to one partition
     */
    double maxLoad;
    double maxLoadLocal;

    /**
     * number of documents that will be sent by the AssignerBolts to the FPTreeJoinerBolts divided by the current
     * window of documents used for partitioning
     */
    double avgCommunication;
    double avgCommunicationLocal;

    /**
     * value that indicates the maximum allowed increase of the avgCommunication. If it is not satisfied
     * recalculation of the partitions will be initiated
     */
    double thresholdAvgCommunication;

    /**
     * value that indicates the maximum allowed increase of the maxLoad. If it is not satisfied
     * recalculation of the partitions will be initiated.
     */
    double thresholdMaxLoad;

    /**
     * map representing the information about the number of joiners to which a document has been emitted
     * key: documentId
     * value: number of Joiners to which the document was sent
     */
    Map<String, Set<Integer>> joinersToWhichDocSent;

    /**
     * map representing the number of documents that every joiner received
     * key: joinerId
     * value: number of documents sent to that particular joiner
     */
    Map<Integer,Integer> numOfDocsReceivedByJoiner;

    /**
     * boolean value indicating that the AssignerBolt has already received a message from one
     * of the other AssignerBolts for recalculating the partitions. Meaning that there is no need
     * for every instance of the AssignerBolt to initiate the recalculation of partitions.
     */
    boolean msgForRecalculatePartitionsAlreadyReceived;
    /**
     * indicating the number of instances that will be created for the WindowCreatorBolt
     */
    int numberOfInstancesForDataPreProcessorBolt;
    /**
     * indicating the number of instances of the WindowCreatorBolts that have sent a msg for finished window
     */
    int numberOfMsgsForFinishedWindowFromDP;
    /**
     * list of all the keys that have been joined
     */
    ArrayList<String> joinedKeys;
    /**
     * counting the number of tuples emitted to every joiner
     */
    int emittingTuplesEverywhere;
    /**
     * the complete number of received documents
     */
    int totalNumOfReceivedDocs;


    /**
     * this method is called before the topology when the topology is finished.
     */
    @Override
    public void cleanup() {
        LOG.info("AssignerBolt " + this.assignerBoltId + " received " + this.totalNumOfReceivedDocs);
        int numberOfKvPairsNotPresent = 0;
        for(String documentName : this.batchOfDocumentsReceivedInBetween.keySet()){
            for(String kvPair : this.batchOfDocumentsReceivedInBetween.get(documentName).getSendByThisKVPairs()){
                if(!this.joinerForKVPair.containsKey(kvPair)){
                    numberOfKvPairsNotPresent+=1;
                }
            }
        }
        LOG.info("AssignerBolt: " + this.assignerBoltId + " not present kvPairs: " + numberOfKvPairsNotPresent);
        LOG.info("AssignerBolt: " + this.assignerBoltId +" not sent: " + this.batchOfDocumentsReceivedInBetween.size());
        LOG.info("AssignerBolt: " + this.assignerBoltId +" emitting tuples everywhere: " + this.emittingTuplesEverywhere);
    }

    public AssignerBolt(int numberOfInstancesForDataPreProcessorBolt){
        this.joinerForKVPair = new HashMap<>();
        this.partitionsCreated = false;
        this.batchOfDocumentsReceivedInBetween = new HashMap<>();
        this.maxLoad = 0;
        this.maxLoadLocal = 0;
        this.avgCommunication = 0;
        this.avgCommunicationLocal = 0;
        this.numOfDocsReceivedByJoiner = new HashMap<>();
        this.joinersToWhichDocSent = new HashMap<String, Set<Integer>>();
        this.thresholdAvgCommunication = 0.2;//threshold for communication overhead is 20%
        this.thresholdMaxLoad = 0.2;//threshold for processing load is 20%
        this.msgForRecalculatePartitionsAlreadyReceived = false;
        this.numberOfTimesKVPairReceived = new HashMap<>();
        this.numberOfInstancesForDataPreProcessorBolt = numberOfInstancesForDataPreProcessorBolt;
        this.numberOfMsgsForFinishedWindowFromDP = 0;
        this.emittingTuplesEverywhere = 0;
        this.totalNumOfReceivedDocs = 0;

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        this.numOfDocumentsReceived = 0;
        //get the id-s of the joiners responsible for working on the created partitions
        this.joinerBoltIds = context.getComponentTasks("fp_tree_joiner_bolt");
        this.joinedKeys = new ArrayList<>();
        //get the id of the instance
        this.assignerBoltId = context.getThisTaskId();

        LOG.info("AssignerBolt " + this.assignerBoltId + " found " + this.joinerBoltIds.size() +" FPTreeJoinerBolts!");
        LOG.info("AssignerBolt " + this.assignerBoltId + " has FPTreeJoinerBolt ids: " + this.joinerBoltIds.toString());
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        //define tick tuples that will indicate that the quality of the partitions should be checked
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 30);
        return conf;
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        //check if the tuple is from the ParserSpout which means that it is a new document
        if(input.getSourceStreamId().equals(CommunicationMessages.store_tuple_msg)){
            //increment the number of documents seen in total
            this.numOfDocumentsReceived+=1;
            this.totalNumOfReceivedDocs+=1;
            /**
             * gather the data from the received tuple
             */
            ArrayList<String> allKVPairs = (ArrayList<String>) input.getValueByField("kv-pairs");
            this.joinedKeys = (ArrayList<String>)input.getValueByField("joined-keys");
            String documentName = input.getStringByField("document");
            String streamId = input.getStringByField("stream");

            //if the partitions have been created and new document is received try to assign that document to joiner
            if(this.partitionsCreated){
                //emit the received document to the assigned JoinerBolts
                boolean emittedByAllKVPairs = emitDocumentToJoiner(collector,documentName,allKVPairs, new ArrayList<>(allKVPairs), false, new HashSet<Integer>());
                //if the document has been emitted by all key-value pairs then remove it from the list of all documents seen in-between
                //if the document is not present only null will be returned
                if(emittedByAllKVPairs){
                    this.batchOfDocumentsReceivedInBetween.remove(documentName);
                }
            }
            //this means that the partitions are creating at the moment so we need to store all of the documents that have been received in the meantime
            else{
                //create a new document from the information received from the Spout
                UpdatePartitionsDocument document = new UpdatePartitionsDocument(documentName, allKVPairs);
                document.setSendByThisKVPairs(new ArrayList<>(allKVPairs));

                //store the document sent in between the creation of partitions and assigning partitions to joiners
                this.batchOfDocumentsReceivedInBetween.put(documentName, document);
            }
        }
        //partitions have been found
        else if(input.getSourceStreamId().equals(CommunicationMessages.partitions_found_msg)){
            //read the information from the message
            ArrayList<HashSet<String>> partitions = (ArrayList<HashSet<String>>) input.getValue(0);
            //read the average communication as computed by the MergerBolt
            this.avgCommunication = input.getDoubleByField("avgCommunication");
            //read the maxLoad as computed by the MergerBolt
            this.maxLoad = input.getDoubleByField("maxLoad");
            //inform that the partitions have been created
            this.partitionsCreated = true;
            //since the partitions have been created inform that no msg has been received for updating the partitions
            this.msgForRecalculatePartitionsAlreadyReceived = false;
            LOG.info("AssignerBolt " + this.assignerBoltId + " partitions size " + partitions.size() );
            //assign the key-value pairs to JoinerBolts
            updateJoinersForKvPair(partitions);

            //check if there have been documents that were received in-between and emit them to the right place
            handleDocumentsReceivedInBetween(collector);

        } else if(input.getSourceStreamId().equals(CommunicationMessages.recalculate_partitions_msg)){
            //indicating that the instance of the AssignerBolt received a message for recalculating the partitions
            this.msgForRecalculatePartitionsAlreadyReceived = true;
        } else if(input.getSourceStreamId().equals(CommunicationMessages.finished_with_all_files)){
            //PreProcessor instance sent a message for finished window
            this.numberOfMsgsForFinishedWindowFromDP+=1;
        } else if(isTickTuple(input)){
            //on predefined time check if the partitions are created and if so calculate the current quality of the partitioning
            if(this.partitionsCreated) {
                /**
                 * first perform the quality check for the current window so that the PartitionCreator and the Merger can be informed
                 * that they need to compute the partitions from the beginning for the next window
                 */
                this.performQualityCheck(collector);

                /**
                 * this means that even though the instance of the AssignerBolt did not conclude decrease in the partitions quality
                 * some other instance has already found a problem in the partitions quality
                 */
                if(this.msgForRecalculatePartitionsAlreadyReceived){
                    this.partitionsCreated = false;
                }

                //the AssignerBolt instance in the next window has no msg for recalculating partitions received from any other AssignerBolt instance
                this.msgForRecalculatePartitionsAlreadyReceived = false;
            }
        }

        if(this.numberOfMsgsForFinishedWindowFromDP == this.numberOfInstancesForDataPreProcessorBolt && this.partitionsCreated && this.batchOfDocumentsReceivedInBetween.size()==0){
            LOG.info("AssignerBolt " + this.assignerBoltId +" informed the joiner that join can be performed");
            //reset the number of msgs for window done received from the WorkerCreatorBolt
            this.numberOfMsgsForFinishedWindowFromDP = 0;
            //inform the FPTreeJoinerBolts that they can proceed to the computation of the joinable documents
            collector.emit(CommunicationMessages.assigner_all_local_docs_sent,new Values(CommunicationMessages.assigner_all_local_docs_sent));
        }
    }

    /**
     * method for emitting the documents to the joiner based on their key-value pairs and if needed to the Merger for updating the partitions.
     * @param collector
     * @param documentName
     * @param keyValuePairs
     * @param kvPairsToSendDocument
     * @param isDocumentAlreadySentToMerger
     * @param allJoinersToWhichDocumentWasAlreadySent
     * @return
     */
    private boolean emitDocumentToJoiner(BasicOutputCollector collector, String documentName, List<String> keyValuePairs, List<String> kvPairsToSendDocument, boolean isDocumentAlreadySentToMerger, Set<Integer> allJoinersToWhichDocumentWasAlreadySent){
        //boolean value indicating whether the document has been emitted for all key-value pairs or if reCalculation was performed for at least one key-value pair
        boolean emittedByAllKVPairs = true;
        //boolean value indicating that the document was already sent to the merger to update the partitions
        boolean sentToMerger = false;
        boolean emittedEverywhere = false;

        for(int i=0;i<kvPairsToSendDocument.size();i++){
            String kvPair = kvPairsToSendDocument.get(i);
            HashSet<Integer> joinersToEmitTo = this.joinerForKVPair.get(kvPair);

            if(joinersToEmitTo==null){
                /**
                 * if a key-value pair hasn't been assigned to any joiner we want to capture
                 * the number of documents in which this kv-pair has appeared.
                 */
                Integer previousValue = this.numberOfTimesKVPairReceived.get(kvPair);
                if(previousValue==null){
                    previousValue = 0;
                }
                //increment the number of documents in which the kv-pair has appeared
                previousValue++;
                //store the number of documents in which the kv-pair has appeared
                this.numberOfTimesKVPairReceived.put(kvPair,previousValue);
                //if the condition for updating the partitions is satisfied inform the merger
                //also if the key is part of the concatenated key then do not emit it to the merger
                if(previousValue >= this.whenToSentMsgToMergerForUpdatingPartitions && !this.joinedKeys.contains(kvPair)){
                    //inform that the document was not emitted by all kv-pairs
                    emittedByAllKVPairs = false;
                    //update the boolean value that the document was sent to the merger
                    sentToMerger = true;
                    break;
                }else{//otherwise assign the kv-pair to all the partitions
                    joinersToEmitTo = new HashSet<>(this.joinerBoltIds);
                    this.joinerForKVPair.put(kvPair,joinersToEmitTo);
                    emittingTuplesEverywhere+=1;
                    emittedEverywhere = true;
                }
            }

            /**
             * if the kv pair is present in any of the partitions
             * or if the kv pair doesn't satisfy the condition for updating the partitions
             * emit it
             */
            if(!sentToMerger) {
                //remove the kvPair by which the document already has been sent
                kvPairsToSendDocument.remove(i);
                //update the index
                i -= 1;

                for (int joinerId : joinersToEmitTo) {
                    //if we have already sent the document to the Joiner contained in the list
                    //of all Joiners to which the document was sent then do NOT send it again
                    if (!allJoinersToWhichDocumentWasAlreadySent.contains(joinerId)) {
                        //update the list of all JoinerBolts to which the document was sent
                        allJoinersToWhichDocumentWasAlreadySent.add(joinerId);
                        //update the maps used for checking the quality of the partitions
                        updateQualityCheckMaps(documentName,joinerId);
                        //EMIT the document to the assigned JoinerBolts for the key-value pairs
                        collector.emitDirect(joinerId, new Values(keyValuePairs, documentName));
                    }
                }

                if(emittedEverywhere) break; //BREAK when the document was emitted by one of its key-value pairs to all joiners
            }
        }

        /**
         * EMIT the document to the MergerBolt for update
         */
        if(sentToMerger){
            //only emit the document to the merger if it has not previously been emitted
            if(!isDocumentAlreadySentToMerger) {
                UpdatePartitionsDocument document = new UpdatePartitionsDocument(documentName, keyValuePairs);
                document.setSendByThisKVPairs(kvPairsToSendDocument);
                //inform that the document is sent to the merger
                isDocumentAlreadySentToMerger = true;
                //store the information in the object representing the document
                document.setDocumentAlreadySentToMerger(isDocumentAlreadySentToMerger);

                if(this.batchOfDocumentsReceivedInBetween.replace(documentName,document)==null){
                    //store the document in the list of documents that appeared in between the calculation of partitions and assigning JoinerBolts for partitions
                    this.batchOfDocumentsReceivedInBetween.put(documentName, document);
                }
                //inform the merger that update of the partitions should be performed by adding the specified document
                //to the already created partitions
                collector.emit(CommunicationMessages.update_partitions_msg, new Values(keyValuePairs, documentName, this.assignerBoltId));
            }
        }

        return emittedByAllKVPairs;
    }

    /**
     * send the documents received in between to the correct component(Merger/Joiner)
     * @param collector
     */
    public void handleDocumentsReceivedInBetween(BasicOutputCollector collector){
        if (this.batchOfDocumentsReceivedInBetween.size() > 0) {
            ArrayList<String> documentsThatNeedToBeRemoved = new ArrayList<>();
            for (String documentName : this.batchOfDocumentsReceivedInBetween.keySet()) {
                UpdatePartitionsDocument document = this.batchOfDocumentsReceivedInBetween.get(documentName);
                List<String> keyValuePairsForDocument = document.getAllKVPairs();
                List<String> kvPairsToSendDocument = document.getSendByThisKVPairs();
                Set<Integer> allJoinersToWhichDocumentWasAlreadySent = document.getJoinersToWhichDocumentSent();
                boolean isDocumentAlreadySentToMerger = document.isDocumentAlreadySentToMerger();
                boolean emittedByAllKVPairs = emitDocumentToJoiner(collector, documentName, keyValuePairsForDocument, kvPairsToSendDocument, isDocumentAlreadySentToMerger, allJoinersToWhichDocumentWasAlreadySent);

                //if the document was emitted by all kv-pairs remove it from the list
                if (emittedByAllKVPairs) {
                    documentsThatNeedToBeRemoved.add(documentName);
                }
            }
            //remove all the documents that have been fully emitted
            for (String documentName : documentsThatNeedToBeRemoved) {
                this.batchOfDocumentsReceivedInBetween.remove(documentName);
            }
        }
    }

    /**
     * method for assigning a joiner for every existing key-value pair from the partitions
     * @param partitions
     */
    public void updateJoinersForKvPair(ArrayList<HashSet<String>> partitions){
        this.joinerForKVPair = new HashMap<>();
        for (int i = 0; i < partitions.size(); i++) {
            for (String kvPair : partitions.get(i)) {
                HashSet<Integer> existingJoinersForKVPair = this.joinerForKVPair.get(kvPair);
                //if there are no existing joiners for the key-value pair then create an empty list of joiners
                if (existingJoinersForKVPair == null) {
                    existingJoinersForKVPair = new HashSet<>();
                }
                //assign the joiners to the key-value pair
                existingJoinersForKVPair.add(this.joinerBoltIds.get(i));
                //store the joiners for the key-value pair
                this.joinerForKVPair.put(kvPair, new HashSet<>(existingJoinersForKVPair));
            }
        }
    }

    /**
     * update the maps used for checking the quality of the partitions
     * @param documentName
     * @param joinerId
     */
    public void updateQualityCheckMaps(String documentName, int joinerId){
        // STEP 1. update the joiners to which the document was sent
        Set<Integer> joinersSentTo = this.joinersToWhichDocSent.get(documentName);
        if (joinersSentTo == null) {
            joinersSentTo = new HashSet<>();
        }
        joinersSentTo.add(joinerId);

        this.joinersToWhichDocSent.put(documentName, joinersSentTo);

        //STEP 2. update the number of documents that were sent to every Joiner Bolt instance
        Integer joinerNumDocs = this.numOfDocsReceivedByJoiner.get(joinerId);
        if (joinerNumDocs == null) {
            joinerNumDocs = 0;
        }
        joinerNumDocs += 1;
        this.numOfDocsReceivedByJoiner.put(joinerId, joinerNumDocs);
    }

    /**
     * method for determining if the repartitioning should be triggered based on the partitions quality
     * @param collector
     */
    private void performQualityCheck(BasicOutputCollector collector){
        long timeForPerformingQualityCheck = System.currentTimeMillis();
        //Check 1. determine if the communication overhead has gotten worse
        int totalNumberOfDocumentsSent = 0;
        int maxEmitted = 0;
        int minEmitted = Integer.MAX_VALUE;

        double median = 0;
        //needed for counting the median
        ArrayList<Integer> allDocumentsEmitted = new ArrayList<>();
        for (String docName : this.joinersToWhichDocSent.keySet()) {
            int numOfJoiners = this.joinersToWhichDocSent.get(docName).size();
            allDocumentsEmitted.add(numOfJoiners);
            //sum up the number of times that all the local documents of the
            //AssignerBolt instance were emitted to the JoinerBolts
            totalNumberOfDocumentsSent += numOfJoiners;

            if(numOfJoiners > maxEmitted){
                maxEmitted = numOfJoiners;
            }
            if(numOfJoiners < minEmitted){
                minEmitted = numOfJoiners;
            }
        }

        this.numOfDocumentsReceived = 0;

        //calculate the avgCommunicationLocal as the total number of times the documents were emitted to the JoinerBolts
        //divided with the number of documents for which the AssignerBolt instance was responsible
        this.avgCommunicationLocal = (totalNumberOfDocumentsSent * 1.0) / (this.joinersToWhichDocSent.keySet().size() * 1.0);
        /**
         * calculate the increase/decrease of the average communication
         */
        //first subtract the original value for the average communication from the local avgCommunication of the AssignerBolt
        double intermediateResultAvgComm = this.avgCommunicationLocal - this.avgCommunication;
        //divide the intermediate result with the original avgCommunication and multiply by 100 in order to express
        //the increase or decrease of the avgCommunication in percentages
        double qualityCheckAvgCommPercentage = (((intermediateResultAvgComm*1.0)) / this.avgCommunication) * 100;

        //Check 2. determine if the load balance has gotten worse
        //get the maximal number of documents assigned to one JoinerBolt instance
        this.maxLoadLocal = 0;
        for(int joinerBoltId : this.numOfDocsReceivedByJoiner.keySet()){
            int numDocsStoredInJoinerInstance = this.numOfDocsReceivedByJoiner.get(joinerBoltId);
            if(numDocsStoredInJoinerInstance > this.maxLoadLocal){
                this.maxLoadLocal = numDocsStoredInJoinerInstance;
            }
        }
        /**
         * calculate the increase/decrease of the load balance
         */
        //first subtract the original value for the maxLoad from the local maxLoad of the AssignerBolt
        double intermediateResultMaxLoad = this.maxLoadLocal - this.maxLoad;
        //divide the intermediate result with the original maxLoad and multiply by 100 in order to express
        //the increase/decrease of the maxLoad in percentages
        double qualityCheckMaxLoadPercentage = ((intermediateResultMaxLoad*1.0)/this.maxLoad) * 100;
        timeForPerformingQualityCheck = System.currentTimeMillis() - timeForPerformingQualityCheck;
        LOG.info("AssignerBolt " + this.assignerBoltId + " time for quality check: " + timeForPerformingQualityCheck + " MS." );

        //check if the avgComm and maxLoad satisfy the threshold
        if((qualityCheckAvgCommPercentage > (this.thresholdAvgCommunication*100)) || (qualityCheckMaxLoadPercentage > (this.thresholdMaxLoad * 100))){
            LOG.info("AssignerBolt " + this.assignerBoltId +" recalculate partitions!!!!" + " avgComm: " + this.avgCommunication + ", avgCommLocal: " + this.avgCommunicationLocal+" !!!!!!!!");
            LOG.info("AssignerBolt " + this.assignerBoltId +" recalculate partitions!!!!" + " maxLoad: " + this.maxLoad + ", maxLoadLocal: " + this.maxLoadLocal + " !!!!!!!!");

            //if either the communication overhead or the load balance has increased more than the predefined threshold
            //inform the MergerBolt that recalculation of partitions needs to be performed
            this.msgForRecalculatePartitionsAlreadyReceived = true;
            //inform the merger that recalculation of partitions should be performed
            collector.emit(CommunicationMessages.recalculate_partitions_msg, new Values(CommunicationMessages.recalculate_partitions_msg));
        }

        /**
         * only inform the Merger that the re-calculation of the partitions was due to the load
         */
        if(qualityCheckMaxLoadPercentage > (this.thresholdMaxLoad * 100)){
            collector.emit(CommunicationMessages.recalculate_partitions_load_msg, new Values(CommunicationMessages.recalculate_partitions_load_msg));
        }

        //clear the data used for the calculation
        this.joinersToWhichDocSent = new HashMap<>();
        this.numOfDocsReceivedByJoiner = new HashMap<>();
        LOG.info("AssignerBolt " + this.assignerBoltId + " clearing data for quality check!!!");
    }

    /**
     * Method for checking if the received tuple is from the type TICK_TUPLE
     * @param tuple
     * @return
     */
    protected static boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
                && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(true,new Fields("document-content","document-name"));
        declarer.declareStream(CommunicationMessages.update_partitions_msg, new Fields("new-document","document-name","assigner-bolt-id"));
        declarer.declareStream(CommunicationMessages.recalculate_partitions_msg, new Fields("msg"));
        declarer.declareStream(CommunicationMessages.recalculate_partitions_load_msg, new Fields("msg"));
        declarer.declareStream(CommunicationMessages.assigner_all_local_docs_sent,new Fields("msg"));
    }
}