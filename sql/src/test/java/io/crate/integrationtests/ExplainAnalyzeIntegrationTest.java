/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.integrationtests;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(numDataNodes = 2)
public class ExplainAnalyzeIntegrationTest extends SQLTransportIntegrationTest {

    @Before
    public void initTestData() throws Exception {
        Setup setup = new Setup(sqlExecutor);
        setup.setUpLocations();
        execute("refresh table locations");
    }

    @Test
    public void testExplainAnalyzeReportsExecutionTimesOnBothNodes() {
        execute("explain analyze select * from locations order by date desc");

        Map<String, Object> analysis = (Map<String, Object>) response.rows()[0][0];
        Map<String, Object> executeAnalysis = (Map<String, Object>) analysis.get("Execute");

        assertThat(executeAnalysis, is(notNullValue()));
        assertTrue(executeAnalysis.keySet().contains("Total"));

        DiscoveryNodes nodes = clusterService().state().nodes();
        for (DiscoveryNode discoveryNode : nodes) {
            if (discoveryNode.isDataNode()) {
                assertThat(executeAnalysis.get(discoveryNode.getId()), is(notNullValue()));
            }
        }
    }

    @Test
    public void testExplainSelectWithoutJobExecutionContexts() {
        execute("explain analyze select 1");
        Map<String, Object> analysis = (Map<String, Object>) response.rows()[0][0];
        Map<String, Object> executeAnalysis = (Map<String, Object>) analysis.get("Execute");
        assertTrue(executeAnalysis.keySet().contains("Total"));
        DiscoveryNodes nodes = clusterService().state().nodes();
        List<Matcher<String>> nodeIds = new ArrayList<>(nodes.getSize());
        for (DiscoveryNode discoveryNode : nodes) {
            nodeIds.add(is(discoveryNode.getId()));
        }
        assertThat(executeAnalysis.keySet(), hasItems(is("Total"), anyOf(nodeIds.toArray(new Matcher[]{}))));
    }
}
