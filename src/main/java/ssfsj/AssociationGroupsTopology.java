package ssfsj;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import ssfsj.bolts.assigner.AssignerBolt;
import ssfsj.bolts.joiner.FPTreeJoinerBolt;
import ssfsj.bolts.merger.MergerAGBolt;
import ssfsj.bolts.partition_creator.PartitionCreatorBolt;
import ssfsj.bolts.pre_processor.PreProcessorBolt;
import ssfsj.spouts.NoBenchSpout;
import ssfsj.utils.CommunicationMessages;

public class AssociationGroupsTopology {
    /**
     * Configuration parameters for the spouts and bolts and the whole topology
     */
    //id representing the stream
    private static final String STREAM_1 = "stream_1";
    //id for spout that reads stream_1
    private static final String spoutStream_1 = "stream_1_spout";
    //number of instances that will be created for the spouts
    private static final int numOfInstancesForSpouts = 1;
    //id representing the partitioner bolt
    private static final String partitioner_bolt_id = "partitioner_bolt";
    //number of instances that will be created for the partitioner bolt
    private static final int numOfInstancesForPartitionerBolt = 8;
    //id representing the AssignerBolt
    private static final String assigner_bolt_id = "assigner_bolt";
    //number of instances that will be created for the assigner bolt
    private static final int numberOfInstancesForAssignerBolt = 6;
    //id representing the MergerBolt
    private static final String merger_bolt_id = "merger_bolt";
    //number of instances that will be created for MergerBolt
    private static final int numberOfInstancesForMergerBolt = 1;
    //id representing the FPTreeJoinerBolt
    private static final String fp_tree_joiner_bolt_id = "fp_tree_joiner_bolt";
    //number of instances that will be created for FPTreeJoinerBolt
    private static final int numberOfInstancesForFpTreeJoinerBolt = 8;
    //id representing the PreProcessor
    private static final String preprocessor_bolt = "data_preprocessor_bolt";
    //number of instances that will be created for the PreProcessorBolt
    private static final int numberOfInstancesForPreProcessorBolt = 1;
    //number of partitions
    private static final int k = 8;
    //perform joining of key-value pairs
    private static final boolean performJoinOfKVPairs = true;
    //the size of the window
    private static final int windowSize = 430000;

    public static void main(String[] args){
        //used to build the toplogy
        TopologyBuilder builder = new TopologyBuilder();

        /**
         * NoBench spout
         */
        String pathToStoringFolder = "/tmp/nb_10mil_more_unique/";
        NoBenchSpout spout_stream_1 = new NoBenchSpout(pathToStoringFolder,STREAM_1,windowSize);
        builder.setSpout(spoutStream_1,spout_stream_1,numOfInstancesForSpouts);

        /**
         * initialization of PreProcessorBolt
         */
        PreProcessorBolt preProcessorBolt = new PreProcessorBolt(k,performJoinOfKVPairs,numOfInstancesForSpouts);
        builder.setBolt(preprocessor_bolt,preProcessorBolt,numberOfInstancesForPreProcessorBolt)
                //PreProcessorBolt should store the incoming tuple from the Spout
                .shuffleGrouping(spoutStream_1, CommunicationMessages.spout_store_tuple_msg)
                //PreProcessorBolt should flush the remaining documents if there are such otherwise do nothing
                .allGrouping(spoutStream_1,CommunicationMessages.spout_finished_with_all_files);

        /**
         * initialization of the PartitionCreatorBolt
         */
        //bolt for receiving the documents from the spout and creating partitions for those documents
        PartitionCreatorBolt partitionCreatorBolt = new PartitionCreatorBolt(windowSize,numOfInstancesForPartitionerBolt);
        builder.setBolt(partitioner_bolt_id,partitionCreatorBolt,numOfInstancesForPartitionerBolt)
                //PartitionCreatorBolt stores the documents sent from the PreProcessorBolt
                .partialKeyGrouping(preprocessor_bolt, CommunicationMessages.store_tuple_msg, new Fields("kv-pairs"))
                //PartitionCreatorBolt uses this message to start partitioning for the current tumbling window
                .allGrouping(preprocessor_bolt,CommunicationMessages.finished_with_all_files)
                //PartitionCreatorBolt is given a message to recalculate the partitions for the currently stored documents
                .allGrouping(assigner_bolt_id,CommunicationMessages.recalculate_partitions_msg);

        /**
         * initialization of the AssignerBolt
         */
        AssignerBolt assignerBolt = new AssignerBolt(numberOfInstancesForPreProcessorBolt);
        builder.setBolt(assigner_bolt_id,assignerBolt,numberOfInstancesForAssignerBolt)
                //AssignerBolt receives the documents PreProcessorBolt
                .shuffleGrouping(preprocessor_bolt,CommunicationMessages.store_tuple_msg)
                //AssignerBolt is informed that the current window has been emitted so he can proceed to informing the JoinerBolt
                .allGrouping(preprocessor_bolt,CommunicationMessages.finished_with_all_files)
                //AssignerBolt received the partitions from the MergerBolt
                .allGrouping(merger_bolt_id, CommunicationMessages.partitions_found_msg)
                //once one AssignerBolt instance detects bad quality of the partitions it informs all necessary components
                .allGrouping(assigner_bolt_id,CommunicationMessages.recalculate_partitions_msg);

        /**
         * initialization of MergerBolt
         */
        MergerAGBolt mergerBolt = new MergerAGBolt(numOfInstancesForPartitionerBolt,k);
        builder.setBolt(merger_bolt_id, mergerBolt, numberOfInstancesForMergerBolt)
                //MergerBolt receives the local partitions from the PartitionCreatorBolt instances
                .allGrouping(partitioner_bolt_id, CommunicationMessages.create_global_partitions_msg)
                //MergerBolt receives a document for updating the partitions from the AssignerBolt instances
                .shuffleGrouping(assigner_bolt_id,CommunicationMessages.update_partitions_msg)
                //MergerBolt receives message from the AssignerBolt about the fact that the partitions are being recreated
                .allGrouping(assigner_bolt_id,CommunicationMessages.recalculate_partitions_msg)
                //just for gathering statistics that the recalculation was because of the load
                .allGrouping(assigner_bolt_id,CommunicationMessages.recalculate_partitions_load_msg);

        /**
         * initialization of FPTreeJoinerBolt
         */
        FPTreeJoinerBolt fpTreeJoinerBolt = new FPTreeJoinerBolt(numberOfInstancesForAssignerBolt);
        builder.setBolt(fp_tree_joiner_bolt_id, fpTreeJoinerBolt, numberOfInstancesForFpTreeJoinerBolt)
                //receive documents from the AssignerBolt
                .directGrouping(assigner_bolt_id)
                //FPTreeJoinerBolt performs the join algorithm once all the documents for the window have been computed (the same can be implemented with tick tuples)
                .allGrouping(assigner_bolt_id, CommunicationMessages.assigner_all_local_docs_sent);

        Config conf = new Config();
        //set to false to disable debug when running on production cluster
        conf.setDebug(false);
        //if there are arguments then we are running on a cluster
        if(args!= null && args.length > 0){
            //parallelism hint to set the number of workers
            conf.setNumWorkers(8);
            //submit the toplogy
            try {
                StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            } catch (AuthorizationException e) {
                e.printStackTrace();
            }
        }else{//we are running locally
            //the maximum number of executors
            conf.setMaxTaskParallelism(10);
            //local cluster used to run locally
            LocalCluster cluster = new LocalCluster();
            //submitting the topology
            cluster.submitTopology("association-groups-based-join",conf, builder.createTopology());
            //sleep
            try {
                Thread.sleep(120000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            //shut down the cluster
            cluster.shutdown();
        }
    }
}
