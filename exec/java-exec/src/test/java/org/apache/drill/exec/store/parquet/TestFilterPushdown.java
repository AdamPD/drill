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
package org.apache.drill.exec.store.parquet;

import org.apache.drill.PlanTestBase;
import org.apache.drill.common.util.TestTools;
import org.junit.Test;

public class TestFilterPushdown extends PlanTestBase {

    static final String WORKING_PATH = TestTools.getWorkingPath();
    static final String TEST_RES_PATH = WORKING_PATH + "/src/test/resources";

    @Test
    public void testSimple() throws Exception {
        test("alter session set `store.parquet.enable_pushdown_filter` = true");

        String query = String.format("select * from dfs_test.`%s/parquet/pushdown` where price > 10.0", TEST_RES_PATH);
        testPlanSubstrPatterns(query, new String[] {"filter=gt(price, 10.0)"}, new String[]{});

        test("alter session set `store.parquet.enable_pushdown_filter` = false");
    }

    @Test
    public void testSimpleDisabled() throws Exception {
        String query = String.format("select * from dfs_test.`%s/parquet/pushdown` where price > 10.0", TEST_RES_PATH);
        testPlanSubstrPatterns(query, new String[]{}, new String[]{"filter=gt(price, 10.0)"});
    }

    @Test
    public void testSimpleNoPushdown() throws Exception {
        test("alter session set `store.parquet.enable_pushdown_filter` = true");

        String query = String.format("select * from dfs_test.`%s/parquet/pushdown` where price > 0.0", TEST_RES_PATH);
        testPlanSubstrPatterns(query, new String[] {}, new String[]{"filter=gt(price, 0.0)"});

        test("alter session set `store.parquet.enable_pushdown_filter` = false");
    }

    @Test
    public void testEliminateFilter() throws Exception {
        test("alter session set `store.parquet.use_new_reader` = true");
        test("alter session set `store.parquet.enable_pushdown_filter` = true");

        String query = String.format("select * from dfs_test.`%s/parquet/pushdown` where price > 10.0", TEST_RES_PATH);
        testPlanSubstrPatterns(query, new String[] {"filter=gt(price, 10.0)"}, new String[]{"Filter(condition=[>($1, 10.0)])"});

        test("alter session set `store.parquet.use_new_reader` = false");
        test("alter session set `store.parquet.enable_pushdown_filter` = false");
    }

    @Test
    public void testPartialPushdown() throws Exception {
        test("alter session set `store.parquet.enable_pushdown_filter` = true");

        String query = String.format("select * from dfs_test.`%s/parquet/pushdown` where price > 10.0 and (price < 20.0 or price + 1 = 11.0)", TEST_RES_PATH);
        testPlanSubstrPatterns(query, new String[] {"filter=gt(price, 10.0)"}, new String[]{});

        test("alter session set `store.parquet.enable_pushdown_filter` = false");
    }

    @Test
    public void testComplex() throws Exception {
        test("alter session set `store.parquet.enable_pushdown_filter` = true");

        String query = String.format("select * from dfs_test.`%s/parquet/pushdown` where (price > 10.0 and price < 15.0) or (price > 12.0 and price < 16.0)", TEST_RES_PATH);
        testPlanSubstrPatterns(query, new String[] {"filter=or(and(gt(price, 10.0), lt(price, 15.0)), and(gt(price, 12.0), lt(price, 16.0)))"}, new String[]{});

        test("alter session set `store.parquet.enable_pushdown_filter` = false");
    }
}
