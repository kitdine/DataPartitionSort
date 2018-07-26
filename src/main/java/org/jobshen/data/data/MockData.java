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

import org.jobshen.data.module.DataBlock;
import org.jobshen.data.module.DataPartition;

/**
 * MockData Description:
 *
 * @author <a href="mailto:kitdnie@gmail.com">Job Shen</a>
 * @version 1.0
 * @date 2018/7/26 23:05
 * @since JDK 1.8
 */
public class MockData {

    public static List<DataPartition> generatorDatas(int partitionSize, int blockMaxSize, int blockMinSize, int arrayMaxSize, int arrayMinSize) {
        Random random = new Random();
        TreeSet<Integer> set = new TreeSet<>();
        List<DataPartition> list = new ArrayList<>(partitionSize);
        int min, max,count = 0;
        for (int i=0;i<partitionSize;i++) {
            DataPartition dp = new DataPartition();
            int blockSize = random.nextInt(blockMaxSize)%(blockMaxSize-blockMinSize+1) + blockMinSize;
            int tmp = Integer.MAX_VALUE / blockSize;
            ArrayList<DataBlock> blockList = new ArrayList<>(blockSize);
            for (int j=0;j<blockSize;j++) {
                min = tmp * j;
                max = tmp * (j+1);
                DataBlock block = new DataBlock();
                int arraySize = random.nextInt(arrayMaxSize)%(arrayMaxSize-arrayMinSize+1) + arrayMinSize;
                for (int k=0;k<arraySize;k++) {
                    set.add(random.nextInt(max)%(max-min+1) + min);
                    count++;
                }
                block.setData(set.toArray(new Integer[0]));
                blockList.add(block);
                set.clear();
            }
            dp.setDataBlocks(blockList);
            list.add(dp);
        }
        System.out.println("size : " + count);
        return list;
    }
}
