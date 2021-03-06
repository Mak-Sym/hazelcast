/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.aggregation;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.hazelcast.aggregation.TestSamples.createEntryWithValue;
import static com.hazelcast.aggregation.TestSamples.createExtractableEntryWithValue;
import static com.hazelcast.aggregation.TestSamples.sampleStrings;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class DistinctAggregationTest {

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testCountAggregator() {
        List<String> values = repeatTimes(3, sampleStrings());
        Set<String> expectation = new HashSet<String>(values);

        Aggregator<Set<String>, String, String> aggregation = Aggregators.distinct();
        for (String value : values) {
            aggregation.accumulate(createEntryWithValue(value));
        }
        Set<String> result = aggregation.aggregate();

        assertThat(result, is(equalTo(expectation)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testCountAggregator_withAttributePath() {
        Person[] people = {new Person(5.1), new Person(3.3)};
        Double[] ages = {5.1, 3.3};
        List<Person> values = repeatTimes(3, Arrays.asList(people));
        Set<Double> expectation = new HashSet<Double>(Arrays.asList(ages));

        Aggregator<Set<Double>, Person, Person> aggregation = Aggregators.distinct("age");
        for (Person value : values) {
            aggregation.accumulate(createExtractableEntryWithValue(value));
        }
        Set<Double> result = aggregation.aggregate();

        assertThat(result, is(equalTo(expectation)));
    }

    private <T> List<T> repeatTimes(int times, List<T> values) {
        List<T> repeatedValues = new ArrayList<T>();
        for (int i = 0; i < times; i++) {
            repeatedValues.addAll(values);
        }
        return repeatedValues;
    }
}
