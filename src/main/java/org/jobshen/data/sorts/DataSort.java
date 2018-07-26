/*
 * Copyright (c) 2018 the original author or authors.
 *   National Electronics and Computer Technology Center, Thailand
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.jobshen.data.sorts;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.LinkedBlockingQueue;

import org.jobshen.data.module.DataBlock;
import org.jobshen.data.module.DataPartition;

/**
 * DataSort Description:
 *
 * @author <a href="mailto:kitdnie@gmail.com">Job Shen</a>
 * @version 1.0
 * @date 2018/7/26 16:33
 * @since JDK 1.8
 */
public class DataSort {

    /**
     * 直接将所有数据导入TreeSet，利用TreeSet的排序功能进行排序
     * @param dataPartitions    dataPartitions x个分区
     * DataPartition 为包含一个 DataBlock 集合的对象，
     * DataBlock 为有序数组结构
     * dataBlock list 为若干个无相交的数据块（dataBlock）有序排列组成
     * @return 排序结果
     */
    public Object[] dataSortSimple(List<DataPartition> dataPartitions) {
        TreeSet<Integer> sortedData = new TreeSet<>();
        for(DataPartition dataPartition : dataPartitions) {
            for(DataBlock dataBlock : dataPartition.getDataBlocks()) {
                sortedData.addAll(Arrays.asList(dataBlock.getData()));
            }
        }
        return sortedData.toArray();
    }

    /**
     *将X个分区数据分别导入无界队列（FIFO）中，分区中数据是有序的，则队列中数据也是有序的
     * 仍然使用TreeSet排序，但是此时TreeSet中数据量要小很多，排序会更快，计算完一部分，则导出结果，清空TreeSet
     * @param dataPartitions    dataPartitions x个分区
     * DataPartition 为包含一个 DataBlock 集合的对象，
     * DataBlock 为有序数组结构
     * dataBlock list 为若干个无相交的数据块（dataBlock）有序排列组成
     * @param allowDuplicate 是否允许重复元素
     * @return 排序结果
     * @throws Exception    忽略异常
     */
    public Object[] dataSort2(List<DataPartition> dataPartitions, boolean allowDuplicate) throws Exception {
        ArrayList<Integer> result = new ArrayList<>();
        // 生成队列
        List<LinkedBlockingQueue<Integer>> partitionQueueList = new ArrayList<>(dataPartitions.size());
        for (DataPartition dataPartition : dataPartitions) {
            LinkedBlockingQueue<Integer> queue = new LinkedBlockingQueue<>();
            for (int j = 0; j < dataPartition.getDataBlocks().size(); j++) {
                DataBlock dataBlock = dataPartition.getDataBlocks().get(j);
                for (int k = 0; k < dataBlock.getData().length; k++) {
                    queue.put(dataBlock.getData()[k]);
                }
            }
            partitionQueueList.add(queue);
        }
        while (partitionQueueList.size() > 0) {
            // 获取第一个队列
            LinkedBlockingQueue<Integer> queue0 = partitionQueueList.get(0);
            while (true) {
                //弹出头元素
                Integer firstData = queue0.poll();
                if (firstData == null) {
                    // 将该队列移除
                    partitionQueueList.remove(0);
                    break;
                }
                // 用个set记录从多个队列中取出的数据，这些数据需要继续排序：
                Set<Integer> sortedData = new TreeSet<>();
                // 先把data放入set：
                sortedData.add(firstData);
                // 倒序获取队列，
                for (int i = partitionQueueList.size() - 1; i > 0; i--) {
                    LinkedBlockingQueue<Integer> queueReverse = partitionQueueList.get(i);
                    while (true) {
                        // 获取头元素，但不删除
                        Integer dataReverse = queueReverse.peek();
                        if (dataReverse == null) {
                            //队列为空 则移除
                            partitionQueueList.remove(i);
                            break;
                        }
                        // dataReverse小于firstData，则把dataReverse放入sortedData，会自动排序
                        if (dataReverse < firstData) {
                            sortedData.add(dataReverse);
                            //删除头元素
                            queueReverse.poll();
                        } else if (dataReverse.equals(firstData)) {
                            if(allowDuplicate) {
                                sortedData.add(dataReverse);
                            }
                            //删除头元素
                            queueReverse.poll();
                            break;
                        } else {
                            break;
                        }
                    }
                }
                // 按firstData查找，小于firstData的值，都已经存入sortedData了
                result.addAll(sortedData);
                sortedData.clear();
            }
        }
        return result.toArray();
    }
}
