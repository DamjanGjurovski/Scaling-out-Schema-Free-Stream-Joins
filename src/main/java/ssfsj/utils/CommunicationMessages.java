package ssfsj.utils;

public class CommunicationMessages {
    /**
     * inform the recipients of the message that the tuple should be stored
     */
    public static final String spout_store_tuple_msg = "spout_store_tuple";
    /**
     * inform the recipients of the message that the tuple should be stored
     */
    public static final String store_tuple_msg = "store_tuple";
    /**
     * inform the recipients of the message that partitions have been created
     */
    public static final String partitions_found_msg = "partitions_found";
    /**
     * inform the recipient of the message that global partitions need to be created
     */
    public static final String create_global_partitions_msg = "create_global_partitions";
    /**
     * inform the recipient of the message that updating of the partitions should be performed
     */
    public static final String update_partitions_msg = "update_partitions";
    /**
     * inform the recipient of the message that recalculation of the partitions should be performed
     */
    public static final String recalculate_partitions_msg="recalculate_partitions";
    /**
     * inform the recipient of the message that recalculation of the partitions should be performed
     * the recalculation is triggered because of inadequate load
     */
    public static final String recalculate_partitions_load_msg="load_recalculate";
    /**
     * inform the JoinerBolt that all documents have been sent by the Assigner meaning
     * that the Joiner can start the join computation
     */
    public static final String assigner_all_local_docs_sent = "assigner_all_local_docs_sent";
    /**
     * inform the recipient of the message that the join algorithm has been finished by the JoinerBolt
     */
    public static final String joiner_join_performed = "joiner_join_performed";
    /**
     * inform the recipients of the message that all the files have been processed by the datepreprocessorbolt
     */
    public static final String finished_with_all_files = "finished_with_all_files";
    /**
     * inform the recipients of the message that all the files have been processed by the spout
     */
    public static final String spout_finished_with_all_files = "spout_finished_with_all_files";
}
