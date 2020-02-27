package ssfsj.utils.sorting;

import ssfsj.utils.KeyValuePair;

public class SortingObject {
    public int position;
    public int value;
    public String key;
    public KeyValuePair keyValuePair;
    public int numberOfDistinctValues;

    public SortingObject(int position, int value){
        this.position = position;
        this.value = value;
    }

    public SortingObject(int position, int value, String key){
        this.position = position;
        this.value = value;
        this.key = key;
    }

    public SortingObject(){

    }

    @Override
    public String toString() {
        return "pos: " + position + ", val: " + value + " ; ";
    }
}
