package com.testproject.data;

public class JoinedDataRow<K, V1, V2> {
    public K key;
    public V1 leftValue;
    public V2 rightValue;

    public JoinedDataRow(K key, V1 leftValue, V2 rightValue){
        this.key = key;
        this.leftValue = leftValue;
        this.rightValue = rightValue;
    }
    public boolean equals(Object other) {
        try {
            JoinedDataRow<K, V1, V2> obj = (JoinedDataRow<K, V1, V2>)other;
            return (this.key == obj.key && this.leftValue == obj.leftValue && this.rightValue == obj.rightValue);
        } catch (Throwable e) {
            return false;
        }
    }
}
