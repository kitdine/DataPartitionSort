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

package org.jobshen.data.data;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.AbortPolicy;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.jobshen.data.module.DataBlock;
import org.jobshen.data.module.DataPartition;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * MockData Description:
 *
 * @author <a href="mailto:kitdnie@gmail.com">Job Shen</a>
 * @version 1.0
 * @date 2018/7/26 23:05
 * @since JDK 1.8
 */
public class MockData {

    public static List<DataPartition> generatorDatas(int partitionSize, int blockMaxSize, int blockMinSize, int arrayMaxSize, int arrayMinSize) throws InterruptedException {
        int cpuSize = Runtime.getRuntime().availableProcessors();
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
            cpuSize * 2,
            cpuSize * 2,
            0L,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingDeque<>(partitionSize),
            new ThreadFactoryBuilder().setNameFormat("data-mock-pool-%d").setDaemon(true).build(),
            new AbortPolicy()
        );
        CountDownLatch countDownLatch = new CountDownLatch(partitionSize);
        Random random = new Random();

        List<DataPartition> list = new ArrayList<>(partitionSize);
        AtomicInteger count = new AtomicInteger(0);
        for (int i=0;i<partitionSize;i++) {
            executor.execute(() -> {
                TreeSet<Integer> set = new TreeSet<>();
                DataPartition dp = new DataPartition();
                int blockSize = random.nextInt(blockMaxSize)%(blockMaxSize-blockMinSize+1) + blockMinSize;
                int tmp = Integer.MAX_VALUE / blockSize;
                int tmpSize = 0;
                ArrayList<DataBlock> blockList = new ArrayList<>(blockSize);
                for (int j=0;j<blockSize;j++) {
                    int min = tmp * j;
                    int max = tmp * (j+1);
                    DataBlock block = new DataBlock();
                    int arraySize = random.nextInt(arrayMaxSize)%(arrayMaxSize-arrayMinSize+1) + arrayMinSize;
                    for (int k=0;k<arraySize;k++) {
                        set.add(random.nextInt(max)%(max-min+1) + min);
                    }
                    tmpSize += arraySize;
                    block.setData(set.toArray(new Integer[0]));
                    blockList.add(block);
                    set.clear();
                }
                dp.setDataBlocks(blockList);
                list.add(dp);
                count.addAndGet(tmpSize);
                countDownLatch.countDown();
            });
        }
        countDownLatch.await();
        System.out.println("size : " + count.get());
        executor.shutdown();
        return list;
    }
}
