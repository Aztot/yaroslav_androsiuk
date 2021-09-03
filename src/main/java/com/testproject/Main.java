package com.testproject;

import com.testproject.data.DataRow;
import com.testproject.data.JoinedDataRow;
import com.testproject.operation.InnerJoinOperation;
import com.testproject.operation.LeftJoinOperation;
import com.testproject.operation.RightJoinOperation;

import java.util.ArrayList;
import java.util.Collection;

public class Main {
    public static void main(String[] args){

        LeftJoinOperation<Integer, String, String> leftJoinOperation = new LeftJoinOperation<>();
        RightJoinOperation<Integer, String, String> rightJoinOperation = new RightJoinOperation<>();
        InnerJoinOperation<Integer, String, String> innerJoinOperation = new InnerJoinOperation<>();

        ArrayList<DataRow<Integer, String>> firstCollection = new ArrayList<>();
        firstCollection.add(new DataRow<>(0, "Ukraine"));
        firstCollection.add(new DataRow<>(1, "Germany"));
        firstCollection.add(new DataRow<>(2, "France"));

        ArrayList<DataRow<Integer, String>> secondCollection = new ArrayList<>();
        secondCollection.add(new DataRow<>(0, "Kyiv"));
        secondCollection.add(new DataRow<>(1, "Berlin"));
        secondCollection.add(new DataRow<>(3, "Budapest"));

        System.out.println("Left join operation");

        Collection<JoinedDataRow<Integer, String, String>> leftResult = leftJoinOperation.join(firstCollection, secondCollection);

        leftResult.forEach(dataRow -> System.out.println(dataRow.key.toString() + dataRow.leftValue + dataRow.rightValue));

        System.out.println("Right join operation");

        Collection<JoinedDataRow<Integer, String, String>> rightResult = rightJoinOperation.join(firstCollection, secondCollection);

        rightResult.forEach(dataRow -> System.out.println(dataRow.key.toString() + dataRow.leftValue + dataRow.rightValue));

        System.out.println("Inner join operation");

        Collection<JoinedDataRow<Integer, String, String>> innerResult = innerJoinOperation.join(firstCollection, secondCollection);

        innerResult.forEach(dataRow -> System.out.println(dataRow.key.toString() + dataRow.leftValue + dataRow.rightValue));
    }
}