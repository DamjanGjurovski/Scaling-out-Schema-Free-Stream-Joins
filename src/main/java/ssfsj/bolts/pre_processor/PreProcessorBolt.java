package ssfsj.bolts.pre_processor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import ssfsj.utils.CommunicationMessages;
import ssfsj.utils.DataPreProcessor;

import java.util.*;

public class PreProcessorBolt extends BaseBasicBolt {
    //id of the instance of the bolt
    int boltId;
    //integer representing the number of partitions that should be created
    int k;
    //map for storing the key-value pairs in which the documents have appeared
    //key: documentId
    //value: set of key-value pairs
    Map<String, HashSet<String>> kvPairsForDocument;
    //the stream id from where the documents come
    String stream_id;
    //boolean value indicating whether joining of the documents should be performed
    boolean performJoining;
    //complete msgs received from the spouts
    int spoutCompleted;
    //the number of spout instances
    int numberOfSpouts;
    //logger for the PreProcessor bolt
    private static final Logger LOG = LogManager.getLogger(PreProcessorBolt.class);

    public PreProcessorBolt(int k,boolean performJoining, int numSpouts){
        this.kvPairsForDocument = new HashMap<>();
        this.k = k;
        this.performJoining = performJoining;
        this.spoutCompleted = 0;
        this.numberOfSpouts = numSpouts;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        this.boltId = context.getThisTaskId();
        LOG.info("PreProcessor Bolt " + this.boltId + " has been created!");
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        if(tuple.getSourceStreamId().equals(CommunicationMessages.spout_store_tuple_msg)){
            /**
             * extract the data from the spout
             */
            ArrayList<String> keyValuePairs = (ArrayList<String>) tuple.getValue(0);
            long docTimestamp = tuple.getLongByField("docTimestamp");
            String documentName = tuple.getStringByField("document");
            String streamId = tuple.getStringByField("stream");
            this.stream_id = streamId;

            this.kvPairsForDocument.put(documentName, new HashSet<>(keyValuePairs));
        }else if(tuple.getSourceStreamId().equals(CommunicationMessages.spout_finished_with_all_files)){
            this.spoutCompleted+=1;
            if(this.spoutCompleted >= this.numberOfSpouts) {
                this.spoutCompleted = 0;
                //map representing the keys from the documents that were combined/concatenated
                ArrayList<String> keysThatAreJoined = new ArrayList<>();
                if (this.performJoining) {
                    //measuring the time needed for pre-processing
                    long timeForDataPreProcessing = System.currentTimeMillis();
                    //create a DataPreProcessor object by indicating the required number of partitions
                    DataPreProcessor dataPreProcessor = new DataPreProcessor(k);
                    /**
                     * set the data
                     */
                    dataPreProcessor.setData(this.kvPairsForDocument);
                    /**
                     * perform the data statistics calculation
                     */
                    dataPreProcessor.calculateDataStatistics();
                    /**
                     * check if the data needs to be modified
                     */
                    keysThatAreJoined = dataPreProcessor.modifyDataIfIncorrectNumberOfPartitions();
                    /**
                     * update the old data with the new one
                     */
                    this.kvPairsForDocument = new HashMap<>(dataPreProcessor.getData());

                    timeForDataPreProcessing = System.currentTimeMillis() - timeForDataPreProcessing;
                    LOG.info("PreProcessorBolt " + this.boltId + "time for data pre-processing: " + timeForDataPreProcessing + " MS.");

                    LOG.info("PreProcessor " + this.boltId + " finished with pre-processing of " + this.kvPairsForDocument.size() + " now emitting documents ");
                    //emit the data to the assigner
                    emit(basicOutputCollector,keysThatAreJoined);
                } else {
                    //there was no joining of keys
                    emit(basicOutputCollector,keysThatAreJoined);
                }
            }

        }

    }

    private void emit(BasicOutputCollector basicOutputCollector, ArrayList<String> keysThatAreJoined){
        //emit the documents for the window
        for (String docName : this.kvPairsForDocument.keySet()) {
            basicOutputCollector.emit(CommunicationMessages.store_tuple_msg, new Values(new ArrayList<>(this.kvPairsForDocument.get(docName)), keysThatAreJoined,
                    docName, new Date().getTime(), this.stream_id));
        }

        LOG.info("PreProcessor emitted everything");

        //release the map
        this.kvPairsForDocument = new HashMap<>();

        /**
         * inform that all the documents have been emitted
         */
        basicOutputCollector.emit(CommunicationMessages.finished_with_all_files, new Values(CommunicationMessages.finished_with_all_files));
        LOG.info("PreProcessor " + this.boltId + " emitted that it is finished");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(CommunicationMessages.store_tuple_msg, new Fields("kv-pairs","joined-keys","document","docTimestamp","stream"));
        outputFieldsDeclarer.declareStream(CommunicationMessages.finished_with_all_files,new Fields("finished_with_all_docs"));
    }
}
