package com.testproject.operation;

import com.testproject.data.DataRow;
import com.testproject.data.JoinedDataRow;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;

public class InnerJoinOperationTest {
    InnerJoinOperation<Integer, Integer, Integer> innerJoinOperation = new InnerJoinOperation<>();

    private void assertArrayNotEquals(Object[] expecteds, Object[] actuals) {
        try {
            assertArrayEquals(expecteds, actuals);
        } catch (AssertionError e) {
            return;
        }
        fail("The arrays are equal");
    }

    @Test
    public void innerJoinSuccess() {
        System.out.println("Inner join operation should succeed");

        ArrayList<DataRow<Integer, Integer>> firstCollection = new ArrayList<>();
        firstCollection.add(new DataRow<>(0, 1));
        firstCollection.add(new DataRow<>(1, 2));

        ArrayList<DataRow<Integer, Integer>> secondCollection = new ArrayList<>();
        secondCollection.add(new DataRow<>(0, 1));
        secondCollection.add(new DataRow<>(1, 2));

        Collection<JoinedDataRow<Integer, Integer, Integer>> result = innerJoinOperation.join(firstCollection, secondCollection);

        ArrayList<JoinedDataRow<Integer, Integer, Integer>> expectedResult = new ArrayList<>();

        expectedResult.add(new JoinedDataRow<>(0, 1, 1));
        expectedResult.add(new JoinedDataRow<>(1, 2, 2));

        assertArrayEquals(result.toArray(), expectedResult.toArray());
    }

    @Test
    public void innerJoinNotFailWithNull() {
        System.out.println("Inner join operation should not fail with null");

        ArrayList<DataRow<Integer, Integer>> firstCollection = new ArrayList<>();
        firstCollection.add(new DataRow<>(0, 1));
        firstCollection.add(new DataRow<>(null, 2));

        ArrayList<DataRow<Integer, Integer>> secondCollection = new ArrayList<>();
        secondCollection.add(new DataRow<>(0, 1));
        secondCollection.add(new DataRow<>(1, null));

        Collection<JoinedDataRow<Integer, Integer, Integer>> result = innerJoinOperation.join(firstCollection, secondCollection);

        ArrayList<JoinedDataRow<Integer, Integer, Integer>> expectedResult = new ArrayList<>();

        expectedResult.add(new JoinedDataRow<>(0, 1, 1));

        assertArrayEquals(result.toArray(), expectedResult.toArray());
    }

    @Test
    public void innerJoinFailWithNullKey() {
        System.out.println("Inner join operation should fail with null key");

        ArrayList<DataRow<Integer, Integer>> firstCollection = new ArrayList<>();
        firstCollection.add(new DataRow<>(0, 1));
        firstCollection.add(new DataRow<>(null, 2));

        ArrayList<DataRow<Integer, Integer>> secondCollection = new ArrayList<>();
        secondCollection.add(new DataRow<>(0, 1));
        secondCollection.add(new DataRow<>(null, 2));

        try {
            innerJoinOperation.join(firstCollection, secondCollection);
        }
        catch (NullPointerException e) {
            return;
        }
        fail("Key cannot be null");

    }

    @Test
    public void innerJoinSuccessWithArray() {
        System.out.println("Inner join operation should success with array");

        InnerJoinOperation<Integer, boolean[], boolean[]> innerJoinOperation = new InnerJoinOperation<>();

        boolean[] firstValue = new boolean[10];
        boolean[] secondValue = new boolean[10];

        firstValue[0] = true;
        firstValue[1] = false;
        secondValue[0] = true;
        secondValue[1] = false;


        ArrayList<DataRow<Integer, boolean[]>> firstCollection = new ArrayList<>();
        firstCollection.add(new DataRow<>(0, firstValue));
        firstCollection.add(new DataRow<>(1, secondValue));

        ArrayList<DataRow<Integer, boolean[]>> secondCollection = new ArrayList<>();
        secondCollection.add(new DataRow<>(0, firstValue));
        secondCollection.add(new DataRow<>(1, secondValue));

        Collection<JoinedDataRow<Integer, boolean[], boolean[]>> result = innerJoinOperation.join(firstCollection, secondCollection);

        ArrayList<JoinedDataRow<Integer, boolean[], boolean[]>> expectedResult = new ArrayList<>();

        expectedResult.add(new JoinedDataRow<>(0, firstValue, firstValue));
        expectedResult.add(new JoinedDataRow<>(1, secondValue, secondValue));

        assertArrayEquals(result.toArray(), expectedResult.toArray());
    }

    @Test
    public void innerJoinFailWhenKeysDuplicates() {
        System.out.println("Inner join operation should fail with duplicated keys");

        ArrayList<DataRow<Integer, Integer>> firstCollection = new ArrayList<>();
        firstCollection.add(new DataRow<>(0, 1));
        firstCollection.add(new DataRow<>(1, 2));
        firstCollection.add(new DataRow<>(1, 3));

        ArrayList<DataRow<Integer, Integer>> secondCollection = new ArrayList<>();
        secondCollection.add(new DataRow<>(0, 1));
        secondCollection.add(new DataRow<>(1, 2));
        secondCollection.add(new DataRow<>(1, 2));

        Collection<JoinedDataRow<Integer, Integer, Integer>> result = innerJoinOperation.join(firstCollection, secondCollection);

        ArrayList<JoinedDataRow<Integer, Integer, Integer>> expectedResult = new ArrayList<>();

        expectedResult.add(new JoinedDataRow<>(0, 1, 1));
        expectedResult.add(new JoinedDataRow<>(1, 2, 2));
        expectedResult.add(new JoinedDataRow<>(1, 2, 3));

        assertArrayNotEquals(expectedResult.toArray(), result.toArray());
    }
}