package org.jobshen.data;

import java.util.Arrays;
import java.util.List;

import org.jobshen.data.data.MockData;
import org.jobshen.data.module.DataPartition;
import org.jobshen.data.sorts.DataSort;

/**
 * Hello world!
 *
 */
public class App {
    public static void main( String[] args ) throws Exception{
        // 6500万 左右数据
        final List<DataPartition> list = MockData.generatorDatas(2000,20,50,100,500);
        DataSort dataSort = new DataSort();
        long start,end;
        start = System.currentTimeMillis();
        dataSort.dataSortSimple(list);
        end = System.currentTimeMillis();
        System.out.println("dataSortSimple cost : " + (end - start));
        start = System.currentTimeMillis();
        dataSort.dataSort2(list, false);
        end = System.currentTimeMillis();
        System.out.println("dataSort2 cost : " + (end - start));
        start = System.currentTimeMillis();
        dataSort.dataSort3(list, false);
        end = System.currentTimeMillis();
        System.out.println("dataSort3 cost : " + (end - start));
        start = System.currentTimeMillis();
       dataSort.dataSort4(list, false);
        end = System.currentTimeMillis();
        System.out.println("dataSort4 cost : " + (end - start));
    }
}
