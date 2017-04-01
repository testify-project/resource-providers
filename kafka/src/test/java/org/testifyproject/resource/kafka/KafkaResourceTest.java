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

import java.util.Map;
import java.util.concurrent.Future;
import kafka.server.KafkaServer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import static org.assertj.core.api.Assertions.assertThat;
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
public class KafkaResourceTest {

    @Cut
    @Fixture(destroy = "stop")
    KafkaResource cut;

    @Test
    public void callToStartResourceShouldReturnRequiredResource() throws Exception {
        TestContext testContext = mock(TestContext.class);
        given(testContext.getName()).willReturn("test");

        Map<String, String> config = cut.configure(testContext);
        assertThat(config).isNotNull();

        ResourceInstance<KafkaServer, KafkaProducer> result = cut.start(testContext, config);

        assertThat(result).isNotNull();
        assertThat(result.getClient()).isPresent();
        assertThat(result.getInstance()).isNotNull();

        KafkaProducer producer = result.getClient().get().getInstance();
        ProducerRecord<String, String> record = new ProducerRecord<>("my-topic", "Test", "test");
        Future response = producer.send(record);
        assertThat(response.get()).isNotNull();
    }

}
