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
package org.testifyproject.resource.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

import java.util.Map;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.testifyproject.LocalResourceInstance;
import org.testifyproject.TestContext;
import org.testifyproject.annotation.LocalResource;
import org.testifyproject.annotation.Sut;
import org.testifyproject.junit4.UnitTest;
import org.testifyproject.trait.PropertiesReader;

import kafka.server.KafkaServer;

/**
 *
 * @author saden
 */
@RunWith(UnitTest.class)
public class KafkaResourceTest {

    @Sut
    KafkaResource sut;

    @Test
    public void callToStartResourceShouldReturnRequiredResource() throws Exception {
        TestContext testContext = mock(TestContext.class);
        LocalResource localResource = mock(LocalResource.class, Answers.RETURNS_MOCKS);
        PropertiesReader configReader = mock(PropertiesReader.class);

        given(testContext.getName()).willReturn("test");

        Map<String, String> config = sut.configure(testContext, localResource,
                configReader);
        assertThat(config).isNotNull();

        LocalResourceInstance<KafkaServer, KafkaProducer> result = sut.start(testContext,
                localResource, config);

        assertThat(result).isNotNull();
        assertThat(result.getClient()).isPresent();
        assertThat(result.getResource()).isNotNull();

        KafkaProducer producer = result.getClient().get().getValue();
        ProducerRecord<String, String> record = new ProducerRecord<>("my-topic", "Test",
                "test");
        Future response = producer.send(record);
        assertThat(response.get()).isNotNull();

        sut.stop(testContext, localResource, result);
    }

}
