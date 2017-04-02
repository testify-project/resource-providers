/*
 * Copyright 2016-2017 Testify Project.
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
package org.testifyproject.resource.titan;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.diskstorage.configuration.backend.CommonsConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import static org.assertj.core.api.Assertions.assertThat;
import org.junit.Test;
import org.junit.runner.RunWith;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import org.testifyproject.LocalResourceInstance;
import org.testifyproject.TestContext;
import org.testifyproject.annotation.Cut;
import org.testifyproject.annotation.Fixture;
import org.testifyproject.junit4.UnitTest;

/**
 *
 * @author saden
 */
@RunWith(UnitTest.class)
public class TitanBerkeleyResourceTest {

    @Cut
    @Fixture(destroy = "stop")
    private TitanBerkeleyResource cut;

    @Test
    public void callToStartResourceShouldReturnRequiredResource() throws Exception {
        TestContext testContext = mock(TestContext.class);
        given(testContext.getName()).willReturn("test");

        CommonsConfiguration config = cut.configure(testContext);
        assertThat(config).isNotNull();

        LocalResourceInstance<TitanGraph, GraphTraversalSource> result = cut.start(testContext, config);

        assertThat(result).isNotNull();
        assertThat(result.getClient()).isNotEmpty();
        assertThat(result.getResource()).isNotNull();

        TitanGraph graph = result.getResource().getInstance();
        graph.addVertex().property("test", "test");
        graph.tx().commit();

        GraphTraversalSource graphTraversalSource = result.getClient().get().getInstance();

        assertThat(graphTraversalSource.V().has("test", "test").next()).isNotNull();
    }

}
