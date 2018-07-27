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
import java.util.concurrent.CompletionService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.AbortPolicy;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.jobshen.data.module.DataBlock;
import org.jobshen.data.module.DataPartition;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

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
    public Integer[] dataSortSimple(List<DataPartition> dataPartitions) {
        TreeSet<Integer> sortedData = new TreeSet<>();
        for(DataPartition dataPartition : dataPartitions) {
            for(DataBlock dataBlock : dataPartition.getDataBlocks()) {
                sortedData.addAll(Arrays.asList(dataBlock.getData()));
            }
        }
        return sortedData.toArray(new Integer[0]);
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
    public Integer[] dataSort2(List<DataPartition> dataPartitions, boolean allowDuplicate) throws Exception {
        // 生成队列
        int listSize = 0;
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
            listSize += queue.size();
        }
        return mergeSortedQueue(partitionQueueList, allowDuplicate, listSize).toArray(new Integer[0]);
    }

    /**
     * 多线程生成队列
     * @param dataPartitions
     * @param allowDuplicate
     * @return
     * @throws Exception
     */
    public Integer[] dataSort3(List<DataPartition> dataPartitions, boolean allowDuplicate) throws Exception {
        int cpuSize = Runtime.getRuntime().availableProcessors();
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
            cpuSize * 2,
            cpuSize * 2,
            0L,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingDeque<>(dataPartitions.size()),
            new ThreadFactoryBuilder().setNameFormat("data-partition-pool-%d").setDaemon(true).build(),
            new AbortPolicy()
        );
        AtomicInteger listSize = new AtomicInteger(0);
        CountDownLatch countDownLatch = new CountDownLatch(dataPartitions.size());

        // 生成队列
        List<LinkedBlockingQueue<Integer>> partitionQueueList = new ArrayList<>(dataPartitions.size());
        for (final DataPartition dataPartition : dataPartitions) {
            executor.execute(() -> {
                LinkedBlockingQueue<Integer> queue = new LinkedBlockingQueue<>();
                buildSingleQueue(dataPartition, queue);
                partitionQueueList.add(queue);
                listSize.addAndGet(queue.size());
                countDownLatch.countDown();
            });
        }
        countDownLatch.await();
        executor.shutdown();
        return mergeSortedQueue(partitionQueueList, allowDuplicate, listSize.get()).toArray(new Integer[0]);
    }

    /**
     * 不及 dataSort3 待优化
     * @param dataPartitions
     * @param allowDuplicate
     * @return
     * @throws Exception
     */
    public Integer[] dataSort4(List<DataPartition> dataPartitions, boolean allowDuplicate) throws Exception {
        int threadSize = Runtime.getRuntime().availableProcessors() * 2;
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
            threadSize,
            threadSize,
            0L,
            TimeUnit.MILLISECONDS,
            new SynchronousQueue<>(),
            new ThreadFactoryBuilder().setNameFormat("data-partition-pool-%d").setDaemon(true).build(),
            new AbortPolicy()
        );
        CompletionService<LinkedBlockingQueue<Integer>> taskCompletionService = new ExecutorCompletionService<>(executor);
        AtomicInteger listSize = new AtomicInteger(0);

        List<LinkedBlockingQueue<Integer>> finalQueueList = new ArrayList<>(threadSize);
        int splitCount = dataPartitions.size() % threadSize == 0 ? dataPartitions.size() / threadSize : dataPartitions.size() / threadSize + 1;
        for(int i = 0; i < threadSize; i++) {
            final int count = i;
            taskCompletionService.submit(() -> {
                LinkedBlockingQueue<Integer> queue = new LinkedBlockingQueue<>();
                AtomicInteger tmpQueueSizes = new AtomicInteger(0);
                int queueSize = (count+1)*splitCount > dataPartitions.size() ? dataPartitions.size() - count*splitCount : splitCount;
                ThreadPoolExecutor tmpExecutor = new ThreadPoolExecutor(
                    threadSize,
                    threadSize,
                    0L,
                    TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<>(),
                    new ThreadFactoryBuilder().setNameFormat("data-pool-"+count+"-%d").setDaemon(true).build(),
                    new AbortPolicy()
                );
                CompletionService<LinkedBlockingQueue<Integer>> tmpTaskService = new ExecutorCompletionService<>(tmpExecutor);
                ArrayList<LinkedBlockingQueue<Integer>> tmpQueueList = Lists.newArrayListWithExpectedSize(queueSize);
                for(int j = 0; j < splitCount; j++) {
                    final int jcount =  j + count*splitCount;
                    tmpTaskService.submit(() -> {
                        LinkedBlockingQueue<Integer> tmpQueue = new LinkedBlockingQueue<>();
                        if(jcount > dataPartitions.size() -1) {
                            return tmpQueue;
                        }
                        DataPartition dataPartition = dataPartitions.get(jcount);
                        buildSingleQueue(dataPartition, tmpQueue);
                        return  tmpQueue;

                    });
                }
                for(int k = 0; k < splitCount; k++) {
                    LinkedBlockingQueue<Integer> future = tmpTaskService.take().get();
                    if (!future.isEmpty()) {
                        tmpQueueList.add(future);
                        tmpQueueSizes.addAndGet(future.size());
                    }
                }
                tmpExecutor.shutdown();
                ArrayList<Integer> tmpList = mergeSortedQueue(tmpQueueList, allowDuplicate, tmpQueueSizes.get());
                for (Integer aTmpList : tmpList) {
                    queue.put(aTmpList);
                }
                listSize.addAndGet(tmpList.size());
                return queue;
            });
        }
        for(int j = 0; j < threadSize; j++) {
            finalQueueList.add(taskCompletionService.take().get());
        }
        executor.shutdown();
        return mergeSortedQueue(finalQueueList, allowDuplicate, listSize.get()).toArray(new Integer[0]);
    }

    private void buildSingleQueue(DataPartition dataPartition, LinkedBlockingQueue<Integer> tmpQueue) {
        for (int j = 0; j < dataPartition.getDataBlocks().size(); j++) {
            DataBlock dataBlock = dataPartition.getDataBlocks().get(j);
            for (int k = 0; k < dataBlock.getData().length; k++) {
                try {
                    tmpQueue.put(dataBlock.getData()[k]);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private ArrayList<Integer> mergeSortedQueue(List<LinkedBlockingQueue<Integer>> list, boolean allowDuplicate, int size) {
        ArrayList<Integer> result = Lists.newArrayListWithExpectedSize(size);
        while (list.size() > 0) {
            // 获取第一个队列
            LinkedBlockingQueue<Integer> queue0 = list.get(0);
            while (true) {
                //弹出头元素
                Integer firstData = queue0.poll();
                if (firstData == null) {
                    // 将该队列移除
                    list.remove(0);
                    break;
                }
                // 用个set记录从多个队列中取出的数据，这些数据需要继续排序：
                Set<Integer> sortedData = new TreeSet<>();
                // 先把data放入set：
                sortedData.add(firstData);
                // 倒序获取队列，
                for (int i = list.size() - 1; i > 0; i--) {
                    LinkedBlockingQueue<Integer> queueReverse = list.get(i);
                    while (true) {
                        // 获取头元素，但不删除
                        Integer dataReverse = queueReverse.peek();
                        if (dataReverse == null) {
                            //队列为空 则移除
                            list.remove(i);
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
        return result;
    }
}
