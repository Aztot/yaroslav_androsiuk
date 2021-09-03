package com.testproject.operation;

import com.testproject.data.DataRow;
import com.testproject.data.JoinedDataRow;

import java.util.ArrayList;
import java.util.Collection;

public class InnerJoinOperation<K, V1, V2> implements JoinOperation<DataRow<K, V1>, DataRow<K, V2>, JoinedDataRow<K, V1, V2>> {
    public Collection<JoinedDataRow<K, V1, V2>> join(Collection<DataRow<K, V1>> leftCollection, Collection<DataRow<K, V2>> rightCollection) {
        Collection<JoinedDataRow<K, V1, V2>> resultCollection = new ArrayList<>();
        for (DataRow<K, V2> rightDataRow : rightCollection){
            for (DataRow<K, V1> leftDataRow : leftCollection){
                if (rightDataRow.key.equals(leftDataRow.key)){
                    resultCollection.add( new JoinedDataRow<>(rightDataRow.key, leftDataRow.value, rightDataRow.value));
                    break;
                }
            }
        }
        return resultCollection ;
    }
}
