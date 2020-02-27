package ssfsj.bolts.partition_creator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import ssfsj.partitioner.AssociationGroupPartitioner;
import ssfsj.utils.CommunicationMessages;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;

public class PartitionCreatorBolt extends BaseBasicBolt {
    //Map representing all of the documents in which a key-value pair has appeared
    LinkedHashMap<String,HashSet<String>> documentsForKVPair;
    //logger for the bolt
    private static final Logger LOG = LogManager.getLogger(PartitionCreatorBolt.class);
    //counting the number of documents that have been received by the PartitionCreatorBolt
    private int numberOfTuplesReceived;
    //integer value indicating after how many documents partitions should be created
    private int whenToCreatePartitions;
    //boolean value indicating whether the partitions should be recomputed or not
    private boolean reComputePartitions;
    //id of the instance of the bolt
    private int boltId;
    //variable indicating how many times the PartitionCreatorBolt instance has created partitions
    private int numberOfTimesPartitionsCreated;
    //number of instances that will be created for the PartitionCreatorBolt
    private int numberOfPartitionCreatorInstances;

    public PartitionCreatorBolt(int windowSize, int numOfInstancesForPartitionCreator){
        this.documentsForKVPair = new LinkedHashMap<>();
        this.numberOfTuplesReceived = 0;
        this.reComputePartitions = true;
        this.numberOfTimesPartitionsCreated = 0;
        this.numberOfPartitionCreatorInstances = numOfInstancesForPartitionCreator;
        //distribute the data evenly among the PartitionCreatorBolts
        this.whenToCreatePartitions = windowSize / this.numberOfPartitionCreatorInstances;
    }

    /**
     * method called when the topology is finished.
     */
    @Override
    public void cleanup() {
        LOG.info("PartitionCreatorBolt " + this.boltId + " computed partitions " + this.numberOfTimesPartitionsCreated + " times.");
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        this.boltId = context.getThisTaskId();
        LOG.info("PartitionBolt " + this.boltId + " has been created!");

    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        //check whether the tuple received is a document that needs to be stored
        if(input.getSourceStreamId().equals(CommunicationMessages.store_tuple_msg)) {
            //only create partitions for pre-specified number of documents for that
            //reason we store only the needed number of documents
            if(this.numberOfTuplesReceived < this.whenToCreatePartitions) {
                /**
                 * get all of the information sent from the spout
                 */
                //current getting the key-value pairs of the current document that can be modified from the DataPreProcessorBolt
                ArrayList<String> allKVPairs = (ArrayList<String>) input.getValueByField("kv-pairs");
                String documentName = input.getStringByField("document");
                long docTimestamp = input.getLongByField("docTimestamp");
                String streamId = input.getStringByField("stream");

                //increment the number of tuples that were received by this bolt
                this.numberOfTuplesReceived += 1;

                /**
                 * update the key-value pairs and the documents in which they have appeared
                 */
                for (String kvPair : allKVPairs) {
                    //take all of the documents that already exist for the key-value pair
                    HashSet<String> docsForKVPair = this.documentsForKVPair.get(kvPair);
                    //if there are no documents then initialize the list of documents
                    if (docsForKVPair == null) {
                        docsForKVPair = new HashSet<>();
                    }
                    //add the current document for the key-value pair
                    docsForKVPair.add(documentName);
                    //update the map of all documents for the key-value pair
                    this.documentsForKVPair.put(kvPair, new HashSet<>(docsForKVPair));
                }
            }

            //if we have received the pre-specified number of documents needed to create partitions
            //and if the partitions should be created then call the method for creating partitions
            if((this.numberOfTuplesReceived==this.whenToCreatePartitions) && this.reComputePartitions){
                //create the partitions
                int resultPartitions = this.createPartitions(collector);

                if(resultPartitions > 0){
                    this.emptyTheData();
                    //inform that the partitions have been computed and recomputation of the partitions should not be done
                    //until the AssignerBolts say so
                    this.reComputePartitions = false;
                }
            }
        } else if(input.getSourceStreamId().equals(CommunicationMessages.finished_with_all_files)){
            LOG.info("PartitionBolt " + this.boltId + " received msg for finished window");
            /**
             * indicates that the WindowCreatorBolt has emitted all the documents and creation of the partitions should be performed
             */
            //if the partitions were not created for a pre-specified number of documents it means
            //that they will be created at the end of the current window
            if(this.reComputePartitions) {
                LOG.info("PartitionBolt " + this.boltId + " computing partitions for window");
                //create the partitions
                int resultPartitions = this.createPartitions(collector);

                if(resultPartitions > 0){
                    this.emptyTheData();
                    //inform that the partitions have been computed and recomputation of the partitions should not be done
                    //until the AssignerBolts say so
                    this.reComputePartitions = false;
                } else{
                    LOG.info("PartitionCreatorBolt " + this.boltId + " should create partitions but nothing is there!");
                }
            } else{
                this.emptyTheData();
                LOG.info("PartitionBolt " + this.boltId + " emptying the window data because re-partition was not triggered!");
            }
        }
        else if(input.getSourceStreamId().equals(CommunicationMessages.recalculate_partitions_msg)){
            if(!this.reComputePartitions) {
                LOG.info("PartitionBolt " + this.boltId + " received message for recalculating the partitions");
            }
            /**
             * indicates that the PartitionCreatorBolt instances have received a message for recalculating
             * the partitions for the current window of documents
             */
            this.reComputePartitions = true;
        }
    }

    /**
     * Method for creating the partitions based on the received documents of the PartitionCreatorBolt instance
     * @return Map<HashSet<String>,HashSet<String>>
     */
    private int createPartitions(BasicOutputCollector collector){
        long timeForComputingLocalParititons = System.currentTimeMillis();
        AssociationGroupPartitioner agPartitioner = new AssociationGroupPartitioner();
        //find the Association groups based on the AssociationGroupPartitioner
        Map<HashSet<String>, HashSet<String>> localPartitions = agPartitioner.documentsPerPartition(this.documentsForKVPair);
        timeForComputingLocalParititons = System.currentTimeMillis() - timeForComputingLocalParititons;
        LOG.info("PartitionBolt " + this.boltId + " partitions were computed for " + this.numberOfTuplesReceived+" tuples.");
        LOG.info("PartitionBolt " + this.boltId + " EqGroups: " + agPartitioner.numberOfFinalEquivalenceGroups + ", AssociationGroups: " + agPartitioner.numberOfFinalAssociationGroups);
        LOG.info("PartitionBolt " + this.boltId + " time for computing local partitions: " + timeForComputingLocalParititons + " MS.");
//        print information about the final association groups computed by the PartitionCreatorBolt
//                this.printFinalPartitions(localPartitions);

        this.numberOfTimesPartitionsCreated+=1;
        LOG.info("PartitionCreatorBolt " + this.boltId + " number of times that partitions were created: " + this.numberOfTimesPartitionsCreated);

        //emit the partitions to the MergerBolt
        collector.emit(CommunicationMessages.create_global_partitions_msg, new Values(localPartitions, this.documentsForKVPair));

        return localPartitions.size();
    }

    /**
     * Resets the received data
     */
    private void emptyTheData(){
        //reset the documents for key-value pair
        this.documentsForKVPair = new LinkedHashMap<>();
        //reset the number of documents received
        this.numberOfTuplesReceived = 0;
    }

    /**
     * Method for printing the final partitions obtained from the partitioners.
     */
    private void printFinalPartitions(Map<HashSet<String>,HashSet<String>> associationGroups){
        int i=0;
        LOG.info("PartitionCreatorBolt " + this.boltId +" has partitions: ");
        for(HashSet<String> documentSet : associationGroups.keySet()){
            LOG.info((i++)+". Partitioner:"+this.boltId+" documents: " + documentSet +", kvPairs: " + associationGroups.get(documentSet));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(CommunicationMessages.create_global_partitions_msg,new Fields("association-groups","documents-per-element"));
    }
}
