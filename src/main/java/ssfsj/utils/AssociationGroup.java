package ssfsj.utils;

import java.util.HashSet;

public class AssociationGroup implements Comparable{
    public HashSet<String> documentsForAssociationGroup;
    public HashSet<String> elementsForAssociationGroup;
    public HashSet<String> impliedDocumentsForAssociationGroup;
    public HashSet<String> impliedElementsForAssociationGroup;

    public AssociationGroup(){
        this.documentsForAssociationGroup = new HashSet<>();
        this.elementsForAssociationGroup = new HashSet<>();
        this.impliedDocumentsForAssociationGroup = new HashSet<>();
        this.impliedElementsForAssociationGroup = new HashSet<>();
    }

    public AssociationGroup(HashSet<String> documentsForAssociationGroup, HashSet<String> elementsForAssociationGroup){
        this.documentsForAssociationGroup = documentsForAssociationGroup;
        this.elementsForAssociationGroup = elementsForAssociationGroup;
        this.impliedDocumentsForAssociationGroup = new HashSet<>();
        this.impliedElementsForAssociationGroup = new HashSet<>();
    }

    public void addAllDocuments(HashSet<String> documents){
        this.documentsForAssociationGroup.addAll(documents);
    }

    public void addAllElements(HashSet<String> elements){
        this.elementsForAssociationGroup.addAll(elements);
    }

    public void addElement(String element){this.elementsForAssociationGroup.add(element);}

    public void addAllImpliedDocuments(HashSet<String> impliedDocuments){
        this.impliedDocumentsForAssociationGroup.addAll(impliedDocuments);
    }

    public void addAllImpliedElements(HashSet<String> impliedElements){
        this.impliedElementsForAssociationGroup.addAll(impliedElements);
    }

    public void addImpliedElement(String impliedElement){
        this.impliedElementsForAssociationGroup.add(impliedElement);
    }

    @Override
    public int compareTo(Object o) {
        AssociationGroup associationGroup = (AssociationGroup) o;
        if(associationGroup.documentsForAssociationGroup.size() > this.documentsForAssociationGroup.size()){
            return 1;
        }else{
            return -1;
        }
    }
}
