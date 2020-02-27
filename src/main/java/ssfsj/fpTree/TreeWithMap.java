package ssfsj.fpTree;

import ssfsj.utils.KeyValuePair;

import java.io.Serializable;
import java.util.*;

public class TreeWithMap implements Serializable {
    private NodeWithMap root;
    public Map<KeyValuePair, List<NodeWithMap>> helperTableAllNodes;
    public static int nodeIds;
    public HashSet<Integer> coveredNodes;
    public HashSet<Integer> branchesToIgnore;

    public TreeWithMap(){
        this.root = new NodeWithMap("root","root");
        this.root.setParent(null);
        this.helperTableAllNodes = new HashMap<>();
        this.coveredNodes = new HashSet<>();
        this.branchesToIgnore = new HashSet<>();
        nodeIds = 1;
    }

    public NodeWithMap getRoot(){
        return this.root;
    }

    /**
     * Method for adding the key-value pairs of the document as nodes in the FP-tree.
     * @param keyValuePairs
     * @param documentId
     */
    public void addNewNodesFromDocument(List<KeyValuePair> keyValuePairs, String documentId){
        ArrayList<NodeWithMap> tmpNodes = new ArrayList<>();
        for(KeyValuePair kvPair : keyValuePairs){
            NodeWithMap node = new NodeWithMap(kvPair);
            tmpNodes.add(node);
        }

        //call the method for finding the position of the nodes in the tree
        addNewNodesFromDocument(tmpNodes, documentId, this.root, 0);
    }

    /**
     * Method for adding a new node in the tree if the node provided as input doesn't exist in the tree.
     * If the node exists in the tree then the children of the node are recursively checked with the input.
     * @param documentNodes
     * @param documentId
     * @param rootNode
     * @param nextKVPairToConsider
     */
    private void addNewNodesFromDocument(ArrayList<NodeWithMap> documentNodes, String documentId, NodeWithMap rootNode, int nextKVPairToConsider){
        //if we have covered all of the nodes provided as input then break
        if(nextKVPairToConsider < documentNodes.size()) {
            //take the next node from the child nodes of the rootNode provided as input
            NodeWithMap rootDocumentNode = documentNodes.get(nextKVPairToConsider);
            //search for the node if it exists in the tree
            NodeWithMap existingNode = searchForNodeWithSameKvPair(rootDocumentNode,rootNode);
            //if the node doesn't exist in the tree then add the node in the tree and also recursively
            //add all of the children of the node
            if (existingNode == null) {
                //call the method for adding the nodes in the tree
                recursivelyAddNewChildrenToRoot(rootDocumentNode, documentNodes, documentId, rootNode, nextKVPairToConsider);
            }
            else {
                nextKVPairToConsider+=1;
                if(nextKVPairToConsider >= documentNodes.size()){
                    //if all of the children nodes have been covered then inform that the document
                    //with id documentId has been covered by storing the document in the last child node
                    existingNode.addDocumentId(documentId);
                }else {
                    //if there are still children left call the same method now with the found node as root
                    addNewNodesFromDocument(documentNodes, documentId, existingNode, nextKVPairToConsider);
                }
            }
        }else{
            return;
        }
    }

    public void addNodesInHelperTable(NodeWithMap node){
        List<NodeWithMap> existingNodes = this.helperTableAllNodes.get(node.getKeyValuePair());
        if(existingNodes==null){
            existingNodes = new LinkedList<>();
        }

        existingNodes.add(node);
        this.helperTableAllNodes.put(node.getKeyValuePair(), existingNodes);

    }

    /**
     * Method for adding nodes as children of the 'rootNode' provided as input. It takes as input the first node ('topNode') of the document ('documentId') that is not
     * present in the FP-tree, the list of all key-value pairs of the document ('documentNodes'), the root node ('rootNode') to which the nodes should be appended and the
     * 'currentIndex' representing the position of the 'topNode' in the list 'documentNodes'.
     * @param topNode
     * @param documentNodes
     * @param documentId
     * @param rootNode
     * @param currentIndex
     */
    public void recursivelyAddNewChildrenToRoot(NodeWithMap topNode, ArrayList<NodeWithMap> documentNodes, String documentId, NodeWithMap rootNode, int currentIndex){
        //if the document has more then one node then use the logic
        if(documentNodes.size() > 1 && currentIndex + 1 < documentNodes.size()) {
            //take the last note from the document nodes ordered by their occurrence in the documents
            NodeWithMap lastNode = documentNodes.get(documentNodes.size() - 1);
            //inform that this nodes and all before it until the root come from document with id 'documentId'
            lastNode.addDocumentId(documentId);
            //update the helper table
            addNodesInHelperTable(lastNode);
            //iterate over all of the other kv-pairs starting from the bottom
            for (int i = documentNodes.size() - 2; i > currentIndex; i--) {
                //take the current node
                NodeWithMap currentNode = documentNodes.get(i);
                //add to it as children the last node
                currentNode.addChild(lastNode,nodeIds);
                nodeIds+=1;
                //update helper table
                addNodesInHelperTable(currentNode);
                //switch the last node with the current node
                lastNode = currentNode;
            }
            //to the top node add the last node as children
            topNode.addChild(lastNode,nodeIds);
            nodeIds+=1;
            //update the helper table
            addNodesInHelperTable(topNode);
            //update the parent node of the top node
            rootNode.addChild(topNode,nodeIds);
            nodeIds+=1;
        }else{//if the document has only one node then add it to the root node provided as input
            //inform that this node comes from document 'documentId'
            topNode.addDocumentId(documentId);
            //update the helper table
            addNodesInHelperTable(topNode);
            //add the node to the root
            rootNode.addChild(topNode,nodeIds);
            nodeIds+=1;
        }
    }

    /**
     * Method for finding if the node provided as input exists in the tree and if so returns the node otherwise
     * returns null
     * @param node
     * @param parentNode
     * @return
     */
    public NodeWithMap searchForNodeWithSameKvPair(NodeWithMap node, NodeWithMap parentNode){
        return parentNode.getChildren().get(node.getKeyValuePair());
    }

    /**
     * Instead of creating a branch store all of the parent ids of the current node.
     * We will use them in the search of the documents in order to determine which nodes should be pruned.
     * @param rootNode
     */
    public void updateNodeDepthUsingNodeIds(NodeWithMap rootNode){
        for(KeyValuePair kvPair : rootNode.getChildren().keySet()){
            NodeWithMap childNode = rootNode.getChildren().get(kvPair);
            childNode.nodeDepth = rootNode.nodeDepth + 1;
            updateNodeDepthUsingNodeIds(childNode);
        }
    }

    /**
     * For given document find all of the joinable documents by iterating through the children of the root
     * @param documentId
     * @param kvPairs
     * @param keys
     * @param joinableDocuments
     * @return
     */
    public Set<String> searchForJoinableThroughChildren(String documentId, HashSet<KeyValuePair> kvPairs, HashSet<String> keys, HashSet<String> joinableDocuments){
        for(KeyValuePair kvPair : this.root.getChildren().keySet()){
            NodeWithMap childOfRoot = this.root.getChildren().get(kvPair);

            boolean containsKey = keys.contains(childOfRoot.getKeyValuePair().getKey());
            boolean containsKvPair = kvPairs.contains(childOfRoot.getKeyValuePair());
            //if the root's child is in conflict then skip the branch that it represents
            if(containsKey && !containsKvPair){
                continue;
            }

            //in order to know whether a join is possible
            //informing that at least one join partner was found
            //and also store documents for the current node if there are some
            HashSet<KeyValuePair> sharedKVPairs = new HashSet<>();
            if(containsKvPair){
                sharedKVPairs.add(childOfRoot.getKeyValuePair());
            }
            //go through the children and search for joinable documents
            joinableDocuments.addAll(childOfRoot.performSearchSecondApproach(kvPairs,keys,joinableDocuments,childOfRoot, sharedKVPairs));

        }

        joinableDocuments.remove(documentId);
        return joinableDocuments;
    }

    /**
     * Method that searches the joinable documents for a given document. As input it gets the numOfKeysInEveryDoc parameter that indicates how many keys appear
     * in all of the documents. By using this information it skips the first "numOfKeysInEveryDoc" levels of the FP-tree by directly getting the key-value
     * pair from the documents and searching for an exact match in the appropriate level of the FP-tree.
     * @param documentId
     * @param kvPairs
     * @param keys
     * @param joinableDocuments
     * @param numOfKeysInEveryDoc
     * @return
     */
    public Set<String> searchingForJoinableDocumentsByUsingKVPairsOfDocument(String documentId, List<KeyValuePair> kvPairs, ArrayList<String> keys,HashSet<String> joinableDocuments, int numOfKeysInEveryDoc){
        NodeWithMap childOfRoot = null;
        NodeWithMap nextNodeToConsider = this.root;
        //based on the key-value pairs of the document iterate through the first "numOfKeysInEveryDoc" levels by getting
        //the child node of the root that corresponds to the next key-value pair in the list of key-value pairs of the document
        for(int i=0;i<numOfKeysInEveryDoc;i++){
            //get the next child of nextNodeToConsider that corresponds to the kvPairs[i] key-value pair
            childOfRoot = nextNodeToConsider.getChildren().get(kvPairs.get(i));

            nextNodeToConsider = childOfRoot;
        }

        //it means that we have skipped some levels of the FPTree
        if(childOfRoot!=null){
            boolean containsKvPair = kvPairs.contains(childOfRoot.getKeyValuePair());

            //in order to know whether a join is possible
            //informing that at least one join partner was found
            //and also store documents for the current node if there are some
            HashSet<KeyValuePair> sharedKVPairs = new HashSet<>();
            if(containsKvPair){
                sharedKVPairs.add(childOfRoot.getKeyValuePair());
            }
            //find the joinable documents by traversing the key-value pairs
            joinableDocuments.addAll(childOfRoot.performSearchSecondApproach(new HashSet<>(kvPairs),new HashSet<>(keys),joinableDocuments,childOfRoot, sharedKVPairs));

            joinableDocuments.remove(documentId);
        }else{
            //it means that there is no key that is present in all of the documents
            joinableDocuments.addAll(searchForJoinableThroughChildren(documentId,new HashSet<>(kvPairs),new HashSet<>(keys),joinableDocuments));
        }

        return joinableDocuments;
    }

    public void printTree(){
        this.root.printNode();
    }

}
