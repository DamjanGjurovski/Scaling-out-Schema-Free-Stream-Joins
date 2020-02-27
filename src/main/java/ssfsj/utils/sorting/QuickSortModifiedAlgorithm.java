package ssfsj.utils.sorting;

import java.util.ArrayList;

public class QuickSortModifiedAlgorithm {
    private ArrayList<SortingObject> myList;
    private int length;

    public void sort(ArrayList<SortingObject> inputArr) {
        if (inputArr == null || inputArr.size() == 0) {
            return;
        }
        this.myList = inputArr;
        length = inputArr.size();
        quickSort(0, length - 1);
    }

    private void quickSort(int lowerIndex, int higherIndex) {

        int i = lowerIndex;
        int j = higherIndex;
        SortingObject sortingObject = myList.get(lowerIndex+(higherIndex-lowerIndex)/2);
        // calculate pivot number, I am taking pivot as middle index number
        int pivot = sortingObject.value;
        int pivotSecondParam = sortingObject.numberOfDistinctValues;

        // Divide into two arrays
        while (i <= j) {
            /**
             * In each iteration, we will identify a number from left side which
             * is greater then the pivot value, and also we will identify a number
             * from right side which is less then the pivot value. Once the search
             * is done, then we exchange both numbers.
             */
            while (myList.get(i).value < pivot) {
                i++;
            }
            while (myList.get(j).value > pivot) {
                j--;
            }

            while (myList.get(i).value == pivot && myList.get(i).numberOfDistinctValues > pivotSecondParam){
                if(myList.get(i+1).value==pivot) {
                    i++;
                }else{
                    break;
                }
            }

            while (myList.get(j).value == pivot && myList.get(j).numberOfDistinctValues < pivotSecondParam){
                j--;
            }

            if (i <= j) {
                exchangeNumbers(i, j);
                //move index to next position on both sides
                i++;
                j--;
            }
        }

        if (lowerIndex < j)
            quickSort(lowerIndex, j);
        if (i < higherIndex)
            quickSort(i, higherIndex);
    }

    private void exchangeNumbers(int i, int j) {
        SortingObject temp = myList.get(i);
        myList.set(i, myList.get(j));
        myList.set(j, temp);
    }
}
