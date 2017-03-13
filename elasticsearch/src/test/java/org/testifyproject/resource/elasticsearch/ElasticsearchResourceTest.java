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
package org.testifyproject.resource.elasticsearch;

import static org.assertj.core.api.Assertions.assertThat;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.junit.Test;
import org.junit.runner.RunWith;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import org.testifyproject.ResourceInstance;
import org.testifyproject.TestContext;
import org.testifyproject.annotation.Cut;
import org.testifyproject.annotation.Fixture;
import org.testifyproject.junit4.UnitTest;

/**
 *
 * @author saden
 */
@RunWith(UnitTest.class)
public class ElasticsearchResourceTest {

    @Cut
    @Fixture(destroy = "stop")
    ElasticsearchResource cut;

    @Test
    public void callToStartResourceShouldReturnRequiredResource() {
        TestContext testContext = mock(TestContext.class);
        given(testContext.getName()).willReturn("test");

        Settings.Builder config = cut.configure(testContext);
        assertThat(config).isNotNull();

        ResourceInstance<Node, Client> result = cut.start(testContext, config);

        assertThat(result).isNotNull();
        assertThat(result.getClient()).isPresent();
        assertThat(result.getServer()).isNotNull();

        Client client = result.getClient().get().getInstance();
        IndexRequestBuilder indexRequestBuilder = client.prepareIndex("test", "test")
                .setSource("{\"message\":\"hello world\"}");

        IndexResponse indexResponse = indexRequestBuilder.get();
        assertThat(indexResponse).isNotNull();
        SearchResponse searchResponse = client.prepareSearch("test").setQuery("{"
                + "    \"query\" : {"
                + "        \"term\" : { \"message\" : \"hello\" }\n"
                + "    }\n"
                + "}")
                .get();
        assertThat(searchResponse).isNotNull();
    }

}
