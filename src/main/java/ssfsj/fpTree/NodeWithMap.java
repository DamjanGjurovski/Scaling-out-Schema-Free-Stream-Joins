package ssfsj.fpTree;

import ssfsj.utils.KeyValuePair;

import java.io.Serializable;
import java.util.*;

public class NodeWithMap implements Serializable {
    private NodeWithMap parent;
    private Map<KeyValuePair, NodeWithMap> children;
    private KeyValuePair keyValuePair;
    private Set<String> documentIds;
    public int nodeDepth;
    public int nodeId;
    public int branchId;

    public NodeWithMap(String key, String value){
        this.keyValuePair = new KeyValuePair(key,value);
        this.children = new HashMap<>();
        this.documentIds = new HashSet<>();
    }

    public NodeWithMap(KeyValuePair kvPair){
        this.keyValuePair = kvPair;
        this.children = new HashMap<>();
        this.documentIds = new HashSet<>();
    }

    public void setParent(NodeWithMap node){
        this.parent = node;
    }

    public NodeWithMap getParent(){
        return this.parent;
    }

    public KeyValuePair getKeyValuePair(){
        return this.keyValuePair;
    }

    public void addDocumentId(String documentId){
        this.documentIds.add(documentId);
    }

    public Set<String> getDocumentIds(){
        return this.documentIds;
    }

    public Map<KeyValuePair, NodeWithMap> getChildren(){return this.children;}

    public void addChild(NodeWithMap childNode, int nodeId){
        childNode.setParent(this);
        childNode.nodeId = nodeId;

        this.children.put(childNode.getKeyValuePair(), childNode);
    }

    /**
     * The method needs to make multiple checks because it can happen that the keys that are immediate children of the root are not present in
     * all of the documents and as a result there will not be a conflict with some branches meaning that as we go down through the leafs we need
     * to make sure that the documents can be joined by at least one key-value pair otherwise we cannot gather all the joinable documents.
     * @param keyValuePairs
     * @param keys
     * @param joinableDocuments
     * @param node
     * @param sharedKVPairs
     * @return
     */

    public HashSet<String> performSearchSecondApproach(HashSet<KeyValuePair> keyValuePairs, HashSet<String> keys, HashSet<String> joinableDocuments, NodeWithMap node, HashSet<KeyValuePair> sharedKVPairs){
        //if there is at least one shared key-value pair with the document then collect all the joinable documents
        if(sharedKVPairs.size() > 0){
            joinableDocuments.addAll(node.documentIds);
        }

        for(KeyValuePair kvPair : node.getChildren().keySet()){
            //create a list storing the temporary shared key-value pairs of
            //the node with the document
            HashSet<KeyValuePair> tmpAddedKvPairs = new HashSet<>();
            NodeWithMap childNode = node.getChildren().get(kvPair);
            boolean containsKey = keys.contains(childNode.getKeyValuePair().getKey());
            boolean containsKvPair = keyValuePairs.contains(childNode.getKeyValuePair());
            if(containsKey && !containsKvPair){
                continue;
            }else{
                //inform that the current examined branch
                //shares key-value pairs with the document
                if(containsKvPair){
                    sharedKVPairs.add(childNode.getKeyValuePair());
                    //keep a temporary array of all the nodes that were added during
                    //the top-down navigation through the branches
                    tmpAddedKvPairs.add(childNode.getKeyValuePair());
                }

                performSearchSecondApproach(keyValuePairs,keys,joinableDocuments,childNode, sharedKVPairs);
                //once a child node has been covered remove all of the key-value pairs
                //for that child node form the list of shared key-value pairs
                sharedKVPairs.removeAll(tmpAddedKvPairs);
            }

        }
        return joinableDocuments;
    }

    /**
     * Method for printing the node together with its children nodes.
     */
    public void printNode(){
        String documentInfo = documentIds.size() > 0 ? documentIds.toString() : "";
        System.out.print("Node: "+nodeId+" Branch: " + branchId+" [Key: " + this.keyValuePair.getKey() + ", Val: " + this.keyValuePair.getValue() + ", Strenght: " + this.keyValuePair.numOfDocsForKey + "] - " + documentInfo);
        System.out.print("\n");

        for(KeyValuePair kvPair : this.children.keySet()){
            NodeWithMap child = this.children.get(kvPair);
            for(int i=0;i<child.nodeDepth;i++){
                System.out.print("\t");
            }
            child.printNode();
        }

        return;

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodeWithMap node = (NodeWithMap) o;
        return Objects.equals(nodeId,node.nodeId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(parent, children, keyValuePair, documentIds);
    }

}
