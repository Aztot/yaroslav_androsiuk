package com.testproject.operation;

import com.testproject.data.DataRow;
import com.testproject.data.JoinedDataRow;

import java.util.ArrayList;
import java.util.Collection;

public class LeftJoinOperation<K, V1, V2> implements JoinOperation<DataRow<K, V1>, DataRow<K, V2>, JoinedDataRow<K, V1, V2>> {
    public Collection<JoinedDataRow<K, V1, V2>> join(Collection<DataRow<K, V1>> leftCollection, Collection<DataRow<K, V2>> rightCollection) {
        Collection<JoinedDataRow<K, V1, V2>> resultCollection = new ArrayList<>();
        for (DataRow<K, V1> leftDataRow : leftCollection){
            JoinedDataRow<K, V1, V2> resultDataRow = new JoinedDataRow<>(leftDataRow.key, leftDataRow.value, null);
            for (DataRow<K, V2> rightDataRow : rightCollection){
                if (leftDataRow.key.equals(rightDataRow.key)){
                    resultDataRow = new JoinedDataRow<>(leftDataRow.key, leftDataRow.value, rightDataRow.value);
                    break;
                }
            }
            resultCollection.add(resultDataRow);
        }
        return resultCollection ;
    }
}
