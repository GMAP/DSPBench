/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamer.examples.trendingtopics;

import com.streamer.base.operator.BaseOperator;
import com.streamer.core.Tuple;
import com.streamer.core.Values;
import com.streamer.examples.trendingtopics.TrendingTopicsConstants.Config;
import com.streamer.examples.utils.ranker.Rankings;
import org.slf4j.Logger;

/**
 * This abstract bolt provides the basic behavior of operators that rank objects according to their count.
 */
public abstract class AbstractRankerOperator extends BaseOperator {
    private static final int DEFAULT_COUNT = 10;

    private int count;
    private Rankings rankings;
    private Tuple firstParent;

    @Override
    public void initialize() {
        count = config.getInt(Config.TOPK, DEFAULT_COUNT);
        rankings = new Rankings(count);
    }

    protected Rankings getRankings() {
        return rankings;
    }

    @Override
    public void onTime() {
        getLogger().debug("Received tick tuple, triggering emit of current rankings");
        emit(firstParent, new Values(rankings.copy()));
        firstParent = null;
        getLogger().debug("Rankings: " + rankings);
    }
    
    @Override
    public final void process(Tuple tuple) {
        updateRankingsWithTuple(tuple);
        
        if (firstParent == null) {
            firstParent = tuple;
        }
    }

    abstract void updateRankingsWithTuple(Tuple tuple);
    abstract Logger getLogger();
}