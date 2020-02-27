package ssfsj.utils;

import java.util.ArrayList;
import java.util.HashSet;

public class AssociationGroupHolderMerger {
    ArrayList<AssociationGroup> associationGroupsWhereKvPairAppears;
    HashSet<String> documentsWhereKvPairAppeared;

    public AssociationGroupHolderMerger(){
        this.associationGroupsWhereKvPairAppears = new ArrayList<>();
        this.documentsWhereKvPairAppeared = new HashSet<>();
    }


    public void addAssociationGroup(HashSet<String> associationGroup, HashSet<String> documentsForAssociationGroup){
        AssociationGroup associationGroup1 = new AssociationGroup();
        associationGroup1.documentsForAssociationGroup = documentsForAssociationGroup;
        associationGroup1.elementsForAssociationGroup = associationGroup;
        this.associationGroupsWhereKvPairAppears.add(associationGroup1);
    }

    public void addDocument(String document){
        this.documentsWhereKvPairAppeared.add(document);
    }

    public void addDocuments(HashSet<String> documents){
        this.documentsWhereKvPairAppeared.addAll(documents);
    }

    public int getPositionOfAGWithLeastElements(){
        int  min= Integer.MAX_VALUE;
        int minPosition = -1;
        for(int i=0; i<this.associationGroupsWhereKvPairAppears.size();i++){
            if(this.associationGroupsWhereKvPairAppears.get(i).elementsForAssociationGroup.size() < min){
                min = this.associationGroupsWhereKvPairAppears.get(i).elementsForAssociationGroup.size();
                minPosition = i;
            }
        }

        return minPosition;
    }

    public HashSet<AssociationGroup> getAGWhereElementWillBeDeleted(){
        HashSet<AssociationGroup> agForDelition = new HashSet<>();
        int positionOfAGThatWontBeDeleted = this.getPositionOfAGWithLeastElements();
        for(int i=0;i<this.associationGroupsWhereKvPairAppears.size();i++){
            if(i!=positionOfAGThatWontBeDeleted){
                agForDelition.add(this.associationGroupsWhereKvPairAppears.get(i));
            }
        }
        return agForDelition;
    }

    public HashSet<String> getDocumentsWhereKvPairAppeared(){
        return this.documentsWhereKvPairAppeared;
    }
}
