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
package org.testify.resource.titan;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.diskstorage.configuration.backend.CommonsConfiguration;
import static org.assertj.core.api.Assertions.assertThat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import org.testify.ResourceInstance;
import org.testify.TestContext;

/**
 *
 * @author saden
 */
public class TitanBerkeleyResourceTest {

    private TitanBerkeleyResource cut;
    private TestContext testContext;
    private CommonsConfiguration configuration;

    @Before
    public void init() {
        cut = new TitanBerkeleyResource();
        testContext = mock(TestContext.class);
        given(testContext.getName()).willReturn("test");
        configuration = cut.configure(testContext);
        assertThat(configuration).isNotNull();
    }

    @After
    public void destory() {
        cut.stop();
    }

    @Test
    public void callToStartResourceShouldReturnRequiredResource() throws Exception {
        ResourceInstance<TitanGraph, Void> result = cut.start(testContext, configuration);

        assertThat(result).isNotNull();
        assertThat(result.getClient()).isEmpty();
        assertThat(result.getServer()).isNotNull();

        TitanGraph graph = result.getServer().getInstance();
        graph.addVertex().property("test", "test");
        graph.tx().commit();

        assertThat(graph.traversal().V().has("test", "test").next()).isNotNull();
    }

}
