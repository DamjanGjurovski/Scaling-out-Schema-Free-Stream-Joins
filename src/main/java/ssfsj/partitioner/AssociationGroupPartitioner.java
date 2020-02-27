package ssfsj.partitioner;

import ssfsj.utils.AssociationGroup;
import ssfsj.utils.Partition;
import ssfsj.utils.comparator.TreeMapCustomComparator;
import ssfsj.utils.sorting.QuickSortAlgorithm;
import ssfsj.utils.sorting.SortingObject;

import java.util.*;

public class AssociationGroupPartitioner {
    public int numberOfFinalAssociationGroups;
    public long finalTimeForAlgorithm;
    public int numberOfFinalEquivalenceGroups;

    /**
     * Method for obtaining association groups of a set of documents.
     * @param documentsPerElement
     * @return
     */
    public Map<HashSet<String>,HashSet<String>> documentsPerPartition( Map<String, HashSet<String>> documentsPerElement){
        //the final list of association groups key-> set of documents; value-> attribute-value pairs for the set of documents
        Map<HashSet<String>,HashSet<String>> associationGroups = new HashMap<>();

        /**
         * finding Equivalence Groups
         */
        //store the equivalence groups in an ordered fashion where the equivalence group with the smallest number of elements will appear first
        LinkedHashMap<HashSet<String>,HashSet<String>> euivalenceGroupsMap = new LinkedHashMap();
        long timeForFindingEquivalenceGroups = System.currentTimeMillis();
        //the idea is to create equivalence groups for the given sets of elements
        //for that reason first we have the documents for every key-value pair and we use that to see which pairs of
        //documents appear together every time in all of the documents. This will represent one equivalence group because it won't have an intersection with any of
        //the other groups that are created.
        //Example. given sets: {A,B,C}, {A,B}, {D,C} . Equivalence groups: {A,B}, {C}, {D}
        for(String kvPair : documentsPerElement.keySet()){
            //all of the documents for a key-value pair
            HashSet<String> documentsForAttribute = new HashSet<>(documentsPerElement.get(kvPair));
            //all of the key-value pairs for the document set
            HashSet<String> kvPairsForDocumentSet = euivalenceGroupsMap.get(documentsForAttribute);
            //check if there are already kv pairs for that set of documents
            if(kvPairsForDocumentSet==null){
                kvPairsForDocumentSet = new HashSet<>();
            }
            //add the new key value pair to the list of pairs for the document set
            kvPairsForDocumentSet.add(kvPair);
            //store the kv pairs and the documents
            euivalenceGroupsMap.put(documentsForAttribute, kvPairsForDocumentSet);
        }
        timeForFindingEquivalenceGroups = System.currentTimeMillis() - timeForFindingEquivalenceGroups;

        //get the number of equivalence groups
        numberOfFinalEquivalenceGroups = euivalenceGroupsMap.size();

        /**
         * assign and order the equivalence groups
         */
        long timeForOrdering = System.currentTimeMillis();
        ArrayList<AssociationGroup> finalEquivalenceGroups = new ArrayList<>();
        ArrayList<SortingObject> arrayForSorting = new ArrayList<>();
        //iterate over the equivalence groups and create a helper array of SortingObjects which will be used for ordering the equivalence groups
        for(HashSet<String> documents : euivalenceGroupsMap.keySet()){
            HashSet<String> elements = new HashSet<>(euivalenceGroupsMap.get(documents));
            AssociationGroup associationGroup = new AssociationGroup(documents, elements);
            finalEquivalenceGroups.add(associationGroup);

            SortingObject sortingObject = new SortingObject(finalEquivalenceGroups.indexOf(associationGroup),associationGroup.documentsForAssociationGroup.size());
            arrayForSorting.add(sortingObject);
        }

        /**
         * order the equivalence groups found in increasing order in terms of the number of elements in every group
         */
        QuickSortAlgorithm quickSortAlgorithm = new QuickSortAlgorithm();
        quickSortAlgorithm.sort(arrayForSorting);

        ArrayList<AssociationGroup> tmpArray = new ArrayList<>();
        for(SortingObject sortingObject : arrayForSorting){
            tmpArray.add(finalEquivalenceGroups.get(sortingObject.position));
        }

        finalEquivalenceGroups = new ArrayList<>(tmpArray);
        //reverse the equivalence groups so that they will be sorted in descending order based on the number of documents
        Collections.reverse(finalEquivalenceGroups);
        tmpArray = new ArrayList<>();
        timeForOrdering = System.currentTimeMillis() - timeForOrdering;

        /**
         * finding subsets of the equivalence groups
         */
        long timeForFindingAssociations = System.currentTimeMillis();
        //create a tree map with reverse ordering meaning that they will be ordered in descending order
        TreeMap<HashSet<String>,HashSet<String>> finalAssociationGroups = new TreeMap<>(new TreeMapCustomComparator().reversed());

        int i=0;

        while (i<finalEquivalenceGroups.size()){
            //take the current equivalences group for which we are checking the subsets
            AssociationGroup associationGroupOutside = finalEquivalenceGroups.get(i);
            //the association group containing all association groups where the associationGroupOutside is subset
            AssociationGroup finalAssociationGroup = new AssociationGroup();
            finalAssociationGroup.addAllDocuments(associationGroupOutside.documentsForAssociationGroup);
            finalAssociationGroup.addAllElements(associationGroupOutside.elementsForAssociationGroup);

            int j=i+1;
            //loop over all of the other equivalence groups to find if the first "association group" is subset on any of them
            while(j < finalEquivalenceGroups.size()){
                AssociationGroup associationGroupInside = finalEquivalenceGroups.get(j);
                if(associationGroupOutside.documentsForAssociationGroup.size() > associationGroupInside.documentsForAssociationGroup.size()) {
                    if (associationGroupOutside.documentsForAssociationGroup.containsAll(associationGroupInside.documentsForAssociationGroup)) {
                        finalAssociationGroup.addAllElements(associationGroupInside.elementsForAssociationGroup);
                        finalAssociationGroup.addAllDocuments(associationGroupInside.documentsForAssociationGroup);

                        finalEquivalenceGroups.remove(j);
                        j--;
                    }
                }
                j++;
            }
            finalAssociationGroups.put(finalAssociationGroup.documentsForAssociationGroup, finalAssociationGroup.elementsForAssociationGroup);
            i++;
        }
        timeForFindingAssociations = System.currentTimeMillis() - timeForFindingAssociations;

        associationGroups = new LinkedHashMap<>(finalAssociationGroups);

        //update the final number of association groups
        numberOfFinalAssociationGroups = associationGroups.keySet().size();

        finalTimeForAlgorithm = timeForFindingEquivalenceGroups + timeForOrdering + timeForFindingAssociations ;

        return associationGroups;
    }

    /**
     * Method for assigning new document to the already computed partitions. The document is assigned to the partition with which it
     * has the most elements in common and the partition that has the least load (at the same time)
     * @param partitions
     * @param kvPairsForDocument
     * @return
     */
    public ArrayList<Partition> assignNewDocumentToPartitions(ArrayList<Partition> partitions, List<String> kvPairsForDocument) {
        int idOfBestPartition = -1;
        int maximalNumberOfElementsInCommon = 0;
        int minimalLoadOfPartition = Integer.MAX_VALUE;

        for (int i = 0; i < partitions.size(); i++) {
            Partition partition = partitions.get(i);

            ArrayList<String> tmpDocument = new ArrayList<>(kvPairsForDocument);
            //get all of the elements in common between the partition and the document
            tmpDocument.retainAll(partition.elementsForPartition);
            //get the load of the partition
            int partitionLoad = partition.numberOfDocumentsForPartition;

            //if the load of the partition is less then any other partition
            //and if the partition has most elements in common with the document
            //then choose that partition
            if (tmpDocument.size() > maximalNumberOfElementsInCommon && partitionLoad < minimalLoadOfPartition) {
                idOfBestPartition = i;
                maximalNumberOfElementsInCommon = tmpDocument.size();
                minimalLoadOfPartition = partitionLoad;
            } else if (partitionLoad < minimalLoadOfPartition && tmpDocument.size() == maximalNumberOfElementsInCommon) {
                idOfBestPartition = i;
                maximalNumberOfElementsInCommon = tmpDocument.size();
                minimalLoadOfPartition = partitionLoad;
            }

        }

        //update the partition chosen
        Partition partition = partitions.get(idOfBestPartition);
        partition.elementsForPartition.addAll(kvPairsForDocument);
        partition.numberOfDocumentsForPartition += 1;

        //store the changed partition
        partitions.set(idOfBestPartition, partition);
        return partitions;
    }

    /**
     * Method for getting the final partitions out of the association groups computed by every PartitionerBolt
     */
    public ArrayList<Partition> getFinalPartitionsByUsingAssocGroupObjects(ArrayList<AssociationGroup> associationGroups, int k){
        //partitions represented through the partition object
        ArrayList<Partition> finalPartitionObjects = new ArrayList<>();

        for(AssociationGroup associationGroup : associationGroups){
            //if we have still not initialized all of the partitions then create a new partition
            if(k>0){
                //create a new partition object
                Partition partition = new Partition(associationGroup.elementsForAssociationGroup, associationGroup.documentsForAssociationGroup);
                finalPartitionObjects.add(partition);
                //reduce the number of free partitions
                k-=1;
            }else{//we have initialized all of the partitions and now we have to fill them up with elements
                //we want to take the partition that has the minimal load
                int partitionWithMinimalLoad = partitionWithMinimalLoadThroughObjects(finalPartitionObjects);

                Partition p = finalPartitionObjects.get(partitionWithMinimalLoad);
                p.addAllElements(associationGroup.elementsForAssociationGroup);
                //get the documents for the association group with maximal load
                p.addAllDocuments(new HashSet<>(associationGroup.documentsForAssociationGroup));

                finalPartitionObjects.set(partitionWithMinimalLoad,p);
            }
        }

        return finalPartitionObjects;
    }

    /**
     * Method for determining the partition with minimal load through partition objects
     */
    public int partitionWithMinimalLoadThroughObjects(ArrayList<Partition> partitions){
        //the partition with minimal load
        int partitionId = -1;
        //the minimal load of a partition
        int minimalLoad = Integer.MAX_VALUE;
        for(int i=0; i < partitions.size();i++){
            Partition p = partitions.get(i);
            //get the load of the partition
            int loadOfPartition = p.loadForPartition();
            if(loadOfPartition<=minimalLoad){
                partitionId = i;
                minimalLoad = loadOfPartition;
            }
        }

        return partitionId;
    }

    public int getNumberOfFinalAssociationGroups(){
        return numberOfFinalAssociationGroups;
    }

    public long getFinalTimeForAlgorithm(){
        return finalTimeForAlgorithm;
    }
}
