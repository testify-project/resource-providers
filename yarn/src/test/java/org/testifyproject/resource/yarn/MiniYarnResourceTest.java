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
package org.testifyproject.resource.yarn;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.testifyproject.LocalResourceInstance;
import org.testifyproject.TestContext;
import org.testifyproject.annotation.LocalResource;
import org.testifyproject.annotation.Sut;
import org.testifyproject.junit4.UnitTest;
import org.testifyproject.trait.PropertiesReader;

/**
 *
 * @author saden
 */
@RunWith(UnitTest.class)
public class MiniYarnResourceTest {

    @Sut
    MiniYarnResource sut;

    @Test
    public void callToStartResourceShouldReturnRequiredResource() throws Exception {
        TestContext testContext = mock(TestContext.class);
        LocalResource localResource = mock(LocalResource.class, Answers.RETURNS_MOCKS);
        PropertiesReader configReader = mock(PropertiesReader.class);

        given(testContext.getName()).willReturn("test");

        YarnConfiguration config = sut.configure(testContext, localResource, configReader);
        assertThat(config).isNotNull();

        LocalResourceInstance<MiniYARNCluster, YarnClient> result = sut.start(testContext,
                localResource, config);

        assertThat(result).isNotNull();
        assertThat(result.getClient()).isPresent();
        assertThat(result.getResource()).isNotNull();

        MiniYARNCluster cluster = result.getResource().getValue();
        assertThat(cluster).isNotNull();

        sut.stop(testContext, localResource, result);

    }

}
