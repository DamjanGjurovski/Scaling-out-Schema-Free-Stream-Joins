package ssfsj.spouts;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import ssfsj.utils.CommunicationMessages;

import java.io.*;
import java.util.*;

public class NoBenchSpout extends BaseRichSpout {
    private static final Logger LOG = LogManager.getLogger(NoBenchSpout.class);
    //collector used to emit output
    SpoutOutputCollector _collector;
    //path to the folder that we would like to read from
    private String pathToFolder;
    //the folder created from the path
    private File folder;
    //list of all the files in the folder
    private String[] listOfFiles;
    //index of the current file that we are reading from
    private int currentFileIndex;
    //the current file that is being processed
    private String currentFile;
    //boolean representing whether we have finished with the reading of the file
    private boolean finishedWithAllFiles;
    //iterator for going through the keys
    private Iterator jsonIterator;
    //enable parallelization of the spout such that multiple spouts can be used to read from the folder
    private ArrayList<String> listOfFilesForSingleInstance;
    //id of the spout instance
    private int spoutId;
    //id of the stream from which the file is read
    private String stream_id;
    //after how many processed documents to pause
    private int whenToSleep;
    //the id of the next document
    private int documentId;
    //number of files read by the spout
    private int numberOfFilesRead;
    //number of tuples emitted by the spout
    private int numberOfTuplesEmitted;
    //boolean informing when all files emitted
    boolean emittedFinishedMsg;

    public NoBenchSpout(String pathToFolder, String stream_id, int whenToSleep) {
        this.pathToFolder = pathToFolder;
        this.stream_id = stream_id;
        //initialize the folder in which all of the files are located
        this.folder = new File(this.pathToFolder);
        //take the names of all of the files in the folder
        this.listOfFiles = folder.list();
        this.finishedWithAllFiles = false;
        this.currentFileIndex = 0;
        this.listOfFilesForSingleInstance = new ArrayList<String>();
        this.numberOfFilesRead = 0;
        this.numberOfTuplesEmitted = 0;
        this.emittedFinishedMsg = false;
        this.whenToSleep = whenToSleep;
        this.documentId = 1;
    }

    /**
     * method called when the topology is killed
     */
    @Override
    public void close() {
        LOG.info("Number of files read: " + this.numberOfFilesRead);
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this._collector = collector;

        //get the id of the spout
        this.spoutId = context.getThisTaskId();
        /**
         * split the files through the instances of the spout
         */
        //number of spout instances
        int spoutsSize = context.getComponentTasks(context.getThisComponentId()).size();
        //get the index of the task
        int taskIndex = context.getThisTaskIndex();

        for (int i = 0; i < this.listOfFiles.length; i++) {
            if (i % spoutsSize == taskIndex) {//if the file is for this spout instance then add it to the list of spouts
                this.listOfFilesForSingleInstance.add(this.listOfFiles[i]);
            }
        }

        //open the file for reading by creating an iterator over the file attributes
        openFileForReading();
    }

    @Override
    public void nextTuple() {
        if (finishedWithAllFiles) {
            if (!emittedFinishedMsg) {//to handle the situation if there is data smaller then the prespecified window
                emittedFinishedMsg = true;
                this._collector.emit(CommunicationMessages.spout_finished_with_all_files, new Values(CommunicationMessages.finished_with_all_files));
            }
            return;
        }

        /**
         * Read the JSON document and emit it based on ALL key value pairs
         */
        HashMap<String, Set<Object>> jsonDocuments = readNextDocument();
        if (jsonDocuments.size() != 0) {
            for (String documentName : jsonDocuments.keySet()) {
                emittedFinishedMsg = false;
                this._collector.emit(CommunicationMessages.spout_store_tuple_msg, new Values(new ArrayList<Object>(jsonDocuments.get(documentName)),
                        documentName, new Date().getTime(), this.stream_id));

                numberOfTuplesEmitted += 1;
                //used in order to simulate a window of data
                if (numberOfTuplesEmitted >= whenToSleep) {//if we have emitted predefined number of tuples (one window) wait for X seconds for the next window to arrive
                    numberOfTuplesEmitted = 0;
                    emittedFinishedMsg = true;
                    //inform the recipient that the spout has finished with emitting a window
                    this._collector.emit(CommunicationMessages.spout_finished_with_all_files, new Values(CommunicationMessages.finished_with_all_files));
                    try {
                        Thread.sleep(40 * 1000);//sleep for X seconds TODO this number needs to be varied depending on the size of the window (for 3 minutes, 6 minutes, 9 minutes)
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

            }
            LOG.info("Spout " + this.spoutId + " read file " + this.listOfFilesForSingleInstance.get(this.currentFileIndex - 1));
        }
    }

    /**
     * Method for parsing a json document
     *
     * @return
     */
    public HashMap<String, Set<Object>> readNextDocument() {
        if (this.finishedWithAllFiles) {
            return null;
        }

        //map of the final json documents for the current file
        HashMap<String, Set<Object>> finalJSONDocuments = new HashMap<>();

        //check whether there are more files to be read
        if (this.currentFileIndex < this.listOfFilesForSingleInstance.size()) {
            //open the next file to be read
            openFileForReading();
            //increment the number of files read
            this.numberOfFilesRead++;
            //Iterate over all of the objects of the jsonArray
            while (this.jsonIterator.hasNext()) {
                //take the next json object from the array
                JSONObject jsonObject = (JSONObject) this.jsonIterator.next();
                //take all of the keys from the json object
                Set keys = jsonObject.keySet();
                //iterator over the keys
                Iterator iterator = keys.iterator();
                //map to store all of the key-values for the current jsonObject
                Set<Object> allKVPairsForCurrentFile = new HashSet<>();
                //create some artificial document name for the jsonObject
                String documentName = this.spoutId + "-file_" + (this.documentId) + ".json";
                this.documentId += 1;
                while (iterator.hasNext()) {
                    //the attribute in the json file
                    String key = (String) iterator.next();
                    Object value;
                    if ((value = jsonObject.get(key)) instanceof JSONObject) {//if the attribute is of type JSONObject concatenate the keys by following the json structure and add the value
                        //if it is important to treat the nested json objects as separate elements
//                            allKVPairsForCurrentFile.addAll(parseJsonObjects(key, (JSONObject) value, allKVPairsForCurrentFile));
                        //since we are looking for equality for the same key, we can also treat everything inside one json object as the whole value
                        value = jsonObject.get(key);
                    } else {
                        if (!key.equals("nested_arr")) {//parsing regular attribute-value pairs
                            //the value for that attribute
                            value = jsonObject.get(key);
                        } else {//parsing of arrays
                            //if two arrays have the same elements but in different order they should still be considered as equal
                            value = new HashSet<>((JSONArray) jsonObject.get(key));
                        }
                        //store the key and the value as a string split with "-" because the analyzed documents do not contain -
                        allKVPairsForCurrentFile.add(key + "-" + value);
                    }
                }
                //update the map of the final JSON documents
                finalJSONDocuments.put(documentName, allKVPairsForCurrentFile);
            }
            currentFileIndex++;
        } else {
            this.finishedWithAllFiles = true;
            this._collector.emit(CommunicationMessages.spout_finished_with_all_files, new Values(CommunicationMessages.finished_with_all_files));
        }

        return finalJSONDocuments;
    }

    /**
     * Method for opening the next file in the folder and creating an iterator so we could access the key-value pairs
     */
    public void openFileForReading() {
        //parser for the json file
        JSONParser jsonParser = new JSONParser();
        //get the array of all of the json objects
        JSONArray jsonArray = null;
        try {
            this.currentFile = this.listOfFilesForSingleInstance.get(currentFileIndex);
            jsonArray = (JSONArray) jsonParser.parse(new FileReader(this.pathToFolder + this.currentFile));

            this.jsonIterator = jsonArray.iterator();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    /**
     * Parse nested json objects. Where the key of every inside element will be concatenated with the key
     * representing the parent json object.
     *
     * @param key
     * @param jsonObject
     * @param allKVPairsForCurrentFile
     * @return
     */
    public Set<Object> parseJsonObjects(String key, JSONObject jsonObject, Set<Object> allKVPairsForCurrentFile) {
        //take all of the keys from the json object
        Set keys = jsonObject.keySet();
        //iterator over the keys
        Iterator iterator = keys.iterator();

        while (iterator.hasNext()) {
            //the attribute in the json file
            String insideKey = (String) iterator.next();
            Object value;

            key = key + "_" + insideKey;

            if ((value = jsonObject.get(insideKey)) instanceof JSONObject) {
                //again there is an object
                parseJsonObjects(key, (JSONObject) value, allKVPairsForCurrentFile);
            } else {
                //the value for that attribute
                value = jsonObject.get(insideKey);
            }

            //store the key and the value as a string split with "-" because the analyzed documents do not contain -
            allKVPairsForCurrentFile.add(key + "-" + value);
        }

        return allKVPairsForCurrentFile;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(CommunicationMessages.spout_store_tuple_msg, new Fields("kv-pairs", "document", "docTimestamp", "stream"));
        declarer.declareStream(CommunicationMessages.spout_finished_with_all_files, new Fields("spout_finished_with_all_docs"));
    }
}
