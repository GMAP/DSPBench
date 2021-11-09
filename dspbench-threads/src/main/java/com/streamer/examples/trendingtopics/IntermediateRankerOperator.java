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

import com.streamer.core.Tuple;
import com.streamer.examples.trendingtopics.TrendingTopicsConstants.Field;
import com.streamer.examples.utils.ranker.Rankable;
import com.streamer.examples.utils.ranker.RankableObjectWithFields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This bolt ranks incoming objects by their count.
 * <p/>
 * It assumes the input tuples to adhere to the following format: (object, object_count, additionalField1,
 * additionalField2, ..., additionalFieldN).
 */
public final class IntermediateRankerOperator extends AbstractRankerOperator {
    private static final Logger LOG = LoggerFactory.getLogger(IntermediateRankerOperator.class);

    @Override
    void updateRankingsWithTuple(Tuple tuple) {
        String topic     = tuple.getString(Field.WORD);
        long count       = tuple.getLong(Field.COUNT);
        int windowLength = tuple.getInt(Field.WINDOW_LENGTH);
        
        Rankable rankable = new RankableObjectWithFields(topic, count, windowLength);
        getRankings().updateWith(rankable);
    }

    @Override
    Logger getLogger() {
        return LOG;
    }
}
