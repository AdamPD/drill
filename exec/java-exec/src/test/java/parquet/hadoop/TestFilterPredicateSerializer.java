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
package parquet.hadoop;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.junit.Assert;
import org.junit.Test;
import parquet.filter2.predicate.FilterApi;
import parquet.filter2.predicate.FilterPredicate;

import java.io.IOException;

public class TestFilterPredicateSerializer {

    @Test
    public void testSimple() throws IOException {
        FilterPredicate predicate = FilterApi.eq(FilterApi.doubleColumn("test"), 1.0);

        ObjectMapper mapper = new ObjectMapper();
        SimpleModule predicateModule = new SimpleModule("FilterPredicateModule", new Version(1, 0, 0, null, null, null));
        predicateModule.addSerializer(FilterPredicate.class, new FilterPredicateSerializer.Se());
        predicateModule.addDeserializer(FilterPredicate.class, new FilterPredicateSerializer.De());
        mapper.registerModule(predicateModule);

        String serialized = mapper.writeValueAsString(predicate);
        FilterPredicate deserialized = mapper.readValue(serialized, FilterPredicate.class);

        Assert.assertEquals(predicate, deserialized);
    }

    @Test
    public void testComplex() throws IOException {
        FilterPredicate predicate = FilterApi.and(FilterApi.eq(FilterApi.floatColumn("test"), 1.0f), FilterApi.and(FilterApi.lt(FilterApi.doubleColumn("test2"), 12.3),
                FilterApi.or(FilterApi.gtEq(FilterApi.longColumn("test3"), 5L), FilterApi.not(FilterApi.lt(FilterApi.intColumn("test4"), 6)))));

        ObjectMapper mapper = new ObjectMapper();
        SimpleModule predicateModule = new SimpleModule("FilterPredicateModule", new Version(1, 0, 0, null, null, null));
        predicateModule.addSerializer(FilterPredicate.class, new FilterPredicateSerializer.Se());
        predicateModule.addDeserializer(FilterPredicate.class, new FilterPredicateSerializer.De());
        mapper.registerModule(predicateModule);

        String serialized = mapper.writeValueAsString(predicate);
        FilterPredicate deserialized = mapper.readValue(serialized, FilterPredicate.class);

        Assert.assertEquals(predicate, deserialized);
    }
}
