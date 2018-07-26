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

package org.jobshen.data.module;

import java.util.ArrayList;

import lombok.Getter;
import lombok.Setter;

/**
 * DataPartition Description:
 *  有序不重复 不一定连续
 *
 * @author <a href="mailto:kitdnie@gmail.com">Job Shen</a>
 * @version 1.0
 * @date 2018/7/26 15:23
 * @since JDK 1.8
 */
@Setter
@Getter
public class DataPartition {

    /**
     * 根据数组下标 从小到大，无相交
     */
    private ArrayList<DataBlock> dataBlocks;
}
