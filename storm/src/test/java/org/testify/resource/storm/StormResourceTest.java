/*
 * Copyright 2016-2017 Sharmarke Aden.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.testify.resource.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;
import static org.assertj.core.api.Assertions.assertThat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import org.testify.ResourceInstance;
import org.testify.TestContext;
import org.testify.resource.fixture.ExclamationTopology;

/**
 *
 * @author saden
 */
public class StormResourceTest {

    private StormResource cut;
    private TestContext testContext;
    private Config config;

    @Before
    public void init() {
        cut = new StormResource();
        testContext = mock(TestContext.class);
        given(testContext.getName()).willReturn("test");
        config = cut.configure(testContext);
        assertThat(config).isNotNull();
    }

    @After
    public void destory() {
        cut.stop();
    }

    @Test
    public void callToStartResourceShouldReturnRequiredResource() {
        ResourceInstance<LocalCluster, Void> result = cut.start(testContext, config);

        assertThat(result).isNotNull();
        assertThat(result.getClient()).isEmpty();
        assertThat(result.getServer()).isNotNull();

        LocalCluster cluster = result.getServer().getInstance();

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("word", new TestWordSpout(), 10);
        builder.setBolt("exclaim1", new ExclamationTopology.ExclamationBolt(), 3).shuffleGrouping("word");
        builder.setBolt("exclaim2", new ExclamationTopology.ExclamationBolt(), 2).shuffleGrouping("exclaim1");

        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(3);

        cluster.submitTopology("exclaim", conf, builder.createTopology());
        Utils.sleep(2000);
        cluster.killTopology("exclaim");
    }

}
