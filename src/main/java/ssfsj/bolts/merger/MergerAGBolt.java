package ssfsj.bolts.merger;

import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.metric.api.CountMetric;
import org.apache.storm.metric.api.ReducedMetric;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import ssfsj.partitioner.AssociationGroupPartitioner;
import ssfsj.utils.*;
import ssfsj.utils.sorting.QuickSortAlgorithm;
import ssfsj.utils.sorting.SortingObject;

import java.util.*;

/**
 * MergerBolt is responsible for creating the final partitions from the local partitions created by the
 * PartitionCreatorBolts.
 * For finding the global AssociationGroups 3 steps are performed:
 *      1. check if the element is contained in a group with less elements and if so
 *         remove the element from the group with more elements (implies removing of subsets)
 *      2. create objects from the Association Groups
 *      3. create the final partitions by assigning the final Association Group objects to the available partitions
 */
public class MergerAGBolt extends BaseBasicBolt {
    //list of all the association groups found from the local partitioners
    Map<HashSet<String>,HashSet<String>> associationGroupsForDocuments;
    //list of the final partitions
    ArrayList<Partition> finalPartitionsWithLoad;
    //int that informs us when all of the PartitionCreatorBolts have sent their local partitions
    int numberOfLocalPartitionsReceived;
    //int representing the number of PartitionCreatorBolt instances
    int numberOfPartitioners;
    //map representing all of the documents and association groups in which a key-value pair has appeared
    HashMap<String, AssociationGroupHolderMerger> documentsForKVPair;
    //the final number of partitions that need to be created
    int k;
    //counter for the number of documents that have been received for assigning to the already created partitions
    int numberOfUpdatesReceived;
    //List of documents that have been received by the AssignerBolts and need to be assigned to partitions
    ArrayList<UpdatePartitionsDocument> documentsThatNeedToBeAssignedToPartitions;
    //number of documents that will be sent by the AssignerBolts to the FPTreeJoinerBolts divided by the current
    //window of documents used for partitioning
    double avgCommunication;
    //maximal number of documents assigned to one partition
    double maxLoad;
    //boolean value used when the recalculation of partitions is performed.
    //If false then the update of the partitions cannot be performed since there are no partitions created.
    boolean partitionsCreated;
    //number of updates of the partitions in total
    private StringBuilder numberOfUpdatesTotal;
    //counting the number of times that the repartitioning was triggered because of inadequate load
    private int numberOfTimesRecalculationTriggeredBecauseLoad;
    //counting the number of times that the repartitioning was triggered because of inadequate communication overhead
    private int numberOfTimesRecalculationTriggeredBecauseCommunication;
    //helper for counting correct
    boolean msgForRecalculatingAlreadyReceived;
    //helper for counting correct
    boolean msgForRecalculatingAlreadyReceivedLoad;
    //create a logger for the merger bolt
    private static final Logger LOG = Logger.getLogger(MergerAGBolt.class);
    /**
     *
     * METRIC DEFINITION for the number of tuples(messages) that were received
     * for new key-value pairs. Metric for the number of updates.
     */
    CountMetric numberOfDocumentsReceivedForUpdate;

    /**
     *END DEFINE METRICS
     */

    public MergerAGBolt(int numberOfpartitioners, int k){
        this.numberOfPartitioners = numberOfpartitioners;
        this.numberOfLocalPartitionsReceived = 0;
        this.associationGroupsForDocuments = new HashMap<>();
        this.documentsForKVPair = new HashMap<>();
        this.finalPartitionsWithLoad = new ArrayList<>();
        this.k = k;
        this.numberOfUpdatesReceived = 0;
        this.documentsThatNeedToBeAssignedToPartitions = new ArrayList<>();
        this.partitionsCreated = false;
        this.numberOfUpdatesTotal = new StringBuilder();
        this.numberOfTimesRecalculationTriggeredBecauseLoad = 0;
        this.numberOfTimesRecalculationTriggeredBecauseCommunication = 0;
        this.msgForRecalculatingAlreadyReceived = false;
        this.msgForRecalculatingAlreadyReceivedLoad = false;
    }

    /**
     * this method is called before the topology is finished.
     */
    @Override
    public void cleanup() {
        LOG.warn("MergerAGBolt received " + this.numberOfDocumentsReceivedForUpdate.getValueAndReset() + " tuples for UPDATE");
        LOG.warn("MergerAGBolt performed " + this.numberOfUpdatesReceived + " actual updates of the partitions");
        LOG.warn("MergerAGBolt has " + this.documentsThatNeedToBeAssignedToPartitions.size() + " documents left");
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        this.numberOfDocumentsReceivedForUpdate = new CountMetric();
    }

    /**
     * The method getComponentConfiguration() enables us to define tick tuples needed for performing the
     * updating of the partitions. TickTuple will be emitted every X seconds and once receiving the tick tuple
     * the merger will perform the update which is needed for assigning the received documents to the existing partitions.
     * @return
     */
    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 20);//emit tick tuple every 20 seconds
        return conf;
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        //if the message received is for creating the global partitions
        if(input.getSourceStreamId().equals(CommunicationMessages.create_global_partitions_msg)) {
            LOG.warn("Received partitions from partitioners");
            //increment the number of local partitions received
            this.numberOfLocalPartitionsReceived += 1;
            /**
             * gather the information from the received tuple
             */
            Map<HashSet<String>,HashSet<String>> localPartitions = (Map<HashSet<String>,HashSet<String>>) input.getValue(0);
            HashMap<String,HashSet<String>> localDocumentsForKVPair = (HashMap<String, HashSet<String>>) input.getValue(1);

            long timeForExecutingGlobalPartitioning = System.currentTimeMillis();

            //add the local partitions to the list of all local partitions from all PartitionCreatorBolts
            this.associationGroupsForDocuments.putAll(localPartitions);

            //iterate over the key-value pairs to set the association groups for every key-value pair
            for(HashSet<String> documents : localPartitions.keySet()){
                HashSet<String> associationGroups =  localPartitions.get(documents);
                for(String kvPair : associationGroups){
                    AssociationGroupHolderMerger associationGroupHolderMerger = this.documentsForKVPair.get(kvPair);
                    if(associationGroupHolderMerger==null){
                        associationGroupHolderMerger = new AssociationGroupHolderMerger();
                    }
                    associationGroupHolderMerger.addAssociationGroup(associationGroups, documents);
                    this.documentsForKVPair.put(kvPair, associationGroupHolderMerger);
                }
            }

            //iterate over the documents to set the documents for ever key-value pair
            for(String kvPair : localDocumentsForKVPair.keySet()){
                AssociationGroupHolderMerger associationGroupHolderMerger = this.documentsForKVPair.get(kvPair);
                if(associationGroupHolderMerger==null){
                    associationGroupHolderMerger = new AssociationGroupHolderMerger();
                }
                associationGroupHolderMerger.addDocuments(localDocumentsForKVPair.get(kvPair));

                this.documentsForKVPair.put(kvPair, associationGroupHolderMerger);
            }

            //if all of the PartitionCreatorBolts have sent their local partitions calculate the global partitions and output them
            if (this.numberOfLocalPartitionsReceived == this.numberOfPartitioners) {
                this.msgForRecalculatingAlreadyReceivedLoad = false;
                this.msgForRecalculatingAlreadyReceived = false;
                LOG.warn("Total number of partitions received EQUALS NUMBER OF PARTITIONERS");

                /**
                 * 1. if an element from an association group with more elements is present in an AG with less elements
                 * remove the element from the AG with more elements. Eliminates duplicate key-value pairs and ag groups that are
                 * a subset of another ag group.
                 */
                long timeForDoingRemoval = System.currentTimeMillis();
                //iterate over all the key-value pairs
                for(String kvPair : this.documentsForKVPair.keySet()){
                    //get all of the association groups from where the key-value pair needs to be eliminated
                    HashSet<AssociationGroup> agWhereKvPairShouldBeRemoved = this.documentsForKVPair.get(kvPair).getAGWhereElementWillBeDeleted();
                    //iterate over all of the association groups where key-value pair needs to be eliminated
                    for(AssociationGroup associationGroup : agWhereKvPairShouldBeRemoved){
                        //eliminate the key-value pair
                        associationGroup.elementsForAssociationGroup.remove(kvPair);
                        //if no more documents are left for the association group then remove it
                        if(associationGroup.elementsForAssociationGroup==null || associationGroup.elementsForAssociationGroup.size()==0){
                            this.associationGroupsForDocuments.remove(associationGroup.documentsForAssociationGroup);
                        }
                    }
                }
                timeForDoingRemoval = System.currentTimeMillis() - timeForDoingRemoval;
                LOG.warn("MergerAGBolt time for doing removal " + timeForDoingRemoval+" MS." );

                /**
                 * 2. create association group objects and order them based on the number of documents in descending order
                 */
                //group together the key-value pairs that appear in the same set of documents
                long timeForGatheringDocs = System.currentTimeMillis();
                Map<HashSet<String>,HashSet<String>> groupKvPairsForSameDocs = new HashMap<>();
                for(HashSet<String> kvPairs : this.associationGroupsForDocuments.values()) {
                    HashSet<String> docsForAssocGroup = new HashSet<>();
                    for (String kvPair : kvPairs) {
                        docsForAssocGroup.addAll(this.documentsForKVPair.get(kvPair).getDocumentsWhereKvPairAppeared());
                    }

                    HashSet<String> existingKvPairs = groupKvPairsForSameDocs.get(docsForAssocGroup);
                    if (existingKvPairs == null) {
                        existingKvPairs = new HashSet<>();
                    }
                    existingKvPairs.addAll(kvPairs);

                    groupKvPairsForSameDocs.put(docsForAssocGroup, existingKvPairs);
                }

                //create objects of the association groups by getting all the documents present in one association group
                ArrayList<AssociationGroup> finalAssociationGroups = new ArrayList<>();
                for(HashSet<String> docsForAG : groupKvPairsForSameDocs.keySet()){
                    finalAssociationGroups.add(new AssociationGroup(docsForAG, groupKvPairsForSameDocs.get(docsForAG)));
                }

                //sort the local partitions in descending order with respect to the number of documents that they have
                ArrayList<SortingObject> arraySort = new ArrayList<>();
                for (int i = 0; i < finalAssociationGroups.size(); i++) {
                    SortingObject sortingObject = new SortingObject(i, finalAssociationGroups.get(i).documentsForAssociationGroup.size());
                    arraySort.add(sortingObject);
                }
                QuickSortAlgorithm quickSortAlgorithm = new QuickSortAlgorithm();
                quickSortAlgorithm.sort(arraySort);

                ArrayList<AssociationGroup> tmpArray = new ArrayList<>();
                for (SortingObject sortingObject : arraySort) {
                    tmpArray.add(finalAssociationGroups.get(sortingObject.position));
                }

                finalAssociationGroups = new ArrayList<>(tmpArray);
                //order the association groups in descending order based on the number of key-value pairs that they have
                Collections.reverse(finalAssociationGroups);
                tmpArray = new ArrayList<>();
                timeForGatheringDocs = System.currentTimeMillis() - timeForGatheringDocs;

                /**
                 *  3. Assign the association groups to partitions. The assigning is done in the following way:
                 *      1. select the association group with most load (bestAG)
                 *      2. add bestAG to the partition with least load or if there are still free, empty partitions left
                 *      add it to an empty partition
                 */
                AssociationGroupPartitioner agPartitioner = new AssociationGroupPartitioner();
                long timeForAssigningAGtoPartitions = System.currentTimeMillis();
                //assign the association groups to partitions
                this.finalPartitionsWithLoad = new ArrayList<>(agPartitioner.getFinalPartitionsByUsingAssocGroupObjects(finalAssociationGroups,k));
                timeForAssigningAGtoPartitions = System.currentTimeMillis()-timeForAssigningAGtoPartitions;
                LOG.warn("Time for assigning AG to partitions in merger: " + timeForAssigningAGtoPartitions+" MS.");
                /**
                 * calculate the partition quality monitoring variables
                 */
                //calculates the maximal load meaning the maximal number of documents that one partition has
                //and the average communication which is the average number of documents sent by a partition
                this.calculateMaxLoadAndAvgCommuncation();
                //clear the documents once the partitioning is computed
                this.documentsForKVPair = new HashMap<>();

                LOG.warn("Number of partitions created: " + finalPartitionsWithLoad.size());

                timeForExecutingGlobalPartitioning = timeForDoingRemoval + timeForAssigningAGtoPartitions + timeForGatheringDocs;
                LOG.warn("Time for computing global partitions in merger: " + timeForExecutingGlobalPartitioning + " MS.");

                //indicate that the partitions have been created by setting the partitionsCreated variable to true
                this.partitionsCreated = true;

                emitFinalPartitions(collector);
            }
        }else if(input.getSourceStreamId().equals(CommunicationMessages.update_partitions_msg)){
            //increment the number of documents received
            this.numberOfDocumentsReceivedForUpdate.incr();
            List<String> newDocument = (List<String>) input.getValueByField("new-document");
            //update the key-value pairs and the documents in which they appear
            String documentName = input.getStringByField("document-name");
            //get the id of the assigner bolt from which the document was received
            int assignerBoltId = input.getIntegerByField("assigner-bolt-id");
            this.numberOfUpdatesReceived += 1;
            UpdatePartitionsDocument updatePartitionsDocument = new UpdatePartitionsDocument(documentName, newDocument);
            this.documentsThatNeedToBeAssignedToPartitions.add(updatePartitionsDocument);
        }
        else if(isTickTuple(input)){
            LOG.warn("Received Tick Tuple " + this.documentsThatNeedToBeAssignedToPartitions.size());
            //perform update of the partitions only if documents from the AssignerBolt instances have been received and the partitions have been created
            if(this.documentsThatNeedToBeAssignedToPartitions.size() > 0 && this.partitionsCreated){
                AssociationGroupPartitioner agPartitioner = new AssociationGroupPartitioner();
                LOG.warn("Number of updates: " + numberOfUpdatesReceived);
                this.numberOfUpdatesTotal.append(" " + numberOfUpdatesReceived);
                long fullTimeForReCalculatingPartitions = System.currentTimeMillis();
                for(UpdatePartitionsDocument updatePartitionsDocument : this.documentsThatNeedToBeAssignedToPartitions) {
                    //recalculate the partitions
                    this.finalPartitionsWithLoad = new ArrayList<>(agPartitioner.assignNewDocumentToPartitions(this.finalPartitionsWithLoad, updatePartitionsDocument.getAllKVPairs()));
                }
                fullTimeForReCalculatingPartitions = System.currentTimeMillis() - fullTimeForReCalculatingPartitions;
                LOG.warn("Time for re-calculating partitions: " + fullTimeForReCalculatingPartitions + " MS.");

                this.numberOfUpdatesReceived = 0;
                this.documentsThatNeedToBeAssignedToPartitions = new ArrayList<>();
                emitFinalPartitions(collector);
            }
        }else if(input.getSourceStreamId().equals(CommunicationMessages.recalculate_partitions_msg)){
            /**
             * indicates that a message for recalculating the partitions has been received
             */
            String msg = input.getString(0);
            //reset the number of partitions received to 0
            this.numberOfLocalPartitionsReceived = 0;

            //reset the local partitions received from the PartitionerBolt instances
            this.associationGroupsForDocuments = new HashMap<>();

            //reset the documents
            this.documentsForKVPair = new HashMap<>();

            LOG.warn("MergerBolt received message for recalculating the partitions");
            if(!this.msgForRecalculatingAlreadyReceived) {
                this.numberOfTimesRecalculationTriggeredBecauseCommunication += 1;
                this.msgForRecalculatingAlreadyReceived = true;
            }
        }
        else if(input.getSourceStreamId().equals(CommunicationMessages.recalculate_partitions_load_msg)){
            if(!this.msgForRecalculatingAlreadyReceivedLoad) {
                this.numberOfTimesRecalculationTriggeredBecauseCommunication -= 1;
                this.numberOfTimesRecalculationTriggeredBecauseLoad += 1;
                this.msgForRecalculatingAlreadyReceivedLoad = true;
            }
        }
    }

    /**
     * Method for calculating the maxLoad and avgCommuncation variables.
     * The calculation of maxLoad is performed by iterating over the partitions
     * and checking which partition has the maximal number of documents
     * assigned to it.
     * The calculation of avgCommunication is performed by checking the average number
     * of documents that will be sent by the assigner which will be accomplished by dividing
     * the total number of documents in every partition with the number of documents used for partitioning.
     */
    public void calculateMaxLoadAndAvgCommuncation(){
        this.maxLoad = 0;
        this.avgCommunication = 0;
        int totalNumberOfDocumentsForPartitions = 0;
        long timeForMaxLoadAndAvgComm = System.currentTimeMillis();
        HashSet<String> uniqueDocuments = new HashSet<>();
        //iterate over the partitions
        for(Partition partition : this.finalPartitionsWithLoad){
            //getting information about all the unique documents (based on documentId) present in all the partitions
            uniqueDocuments.addAll(partition.documentsInWhichAllElementsAppear);
            //getting the number of documents for a particular partition
            int numOfDocsForPartition = partition.documentsInWhichAllElementsAppear.size();
            //summing up the total number of documents in every partition
            totalNumberOfDocumentsForPartitions+=numOfDocsForPartition;
            //check if the num of docs for the partition satisfies the condition
            if(numOfDocsForPartition > this.maxLoad){
                this.maxLoad = numOfDocsForPartition;
            }
        }
        this.avgCommunication = (totalNumberOfDocumentsForPartitions*1.0)/(uniqueDocuments.size()*1.0);
        timeForMaxLoadAndAvgComm = System.currentTimeMillis() - timeForMaxLoadAndAvgComm;

        LOG.warn("---------------------------------------------------------------------");
        LOG.warn("Total number of documents used for partitioning: " + uniqueDocuments.size() + ", per partitions: "+ totalNumberOfDocumentsForPartitions);
        LOG.warn("Maximal load of partition: " + this.maxLoad);
        LOG.warn("Average communication of partitions: " + this.avgCommunication);
        LOG.warn("Time for calculating maxLoad and avgCommunication: " + timeForMaxLoadAndAvgComm + " MS");
        LOG.warn("---------------------------------------------------------------------");
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

    /**
     * Method for emitting the partitions to the connected components in the topology
     * @param collector
     */
    public void emitFinalPartitions(BasicOutputCollector collector){
        //list of only the final partitions
        ArrayList<HashSet<String>> finalPartitions = new ArrayList<>();

        for(Partition p : this.finalPartitionsWithLoad){
            //update the list of all partitions
            finalPartitions.add(new HashSet<>(p.elementsForPartition));
        }
        //emit the final partitions to the assigner
        collector.emit(CommunicationMessages.partitions_found_msg, new Values(finalPartitions,this.maxLoad,this.avgCommunication));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(CommunicationMessages.partitions_found_msg,new Fields("partitions","maxLoad","avgCommunication"));
    }
}
