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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.SystemTime$;
import org.apache.curator.test.TestingServer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.testifyproject.ResourceInstance;
import org.testifyproject.ResourceProvider;
import org.testifyproject.TestContext;
import org.testifyproject.core.ResourceInstanceBuilder;
import org.testifyproject.core.util.FileSystemUtil;
import scala.Option;

/**
 * An implementation of ResourceProvider that provides a local ZooKeeper test
 * server and client using Apache Curator.
 *
 * @author saden
 */
public class KafkaResource implements ResourceProvider<Map<String, String>, KafkaServer, KafkaProducer> {

    private final FileSystemUtil fileSystemUtil = FileSystemUtil.INSTANCE;
    private KafkaServer server;
    private KafkaProducer<Object, Object> client;
    private TestingServer zkServer;

    @Override
    public Map<String, String> configure(TestContext testContext) {
        String testName = testContext.getName();
        String logDir = fileSystemUtil.createPath("target", "kafka", testName);

        Map<String, String> brokerConfig = new HashMap<>();
        brokerConfig.put("broker.id", "0");
        brokerConfig.put("zookeeper.connect", "localhost:0");
        brokerConfig.put("log.dir", logDir);
        brokerConfig.put("port", "0");

        return brokerConfig;
    }

    @Override
    public ResourceInstance<KafkaServer, KafkaProducer> start(TestContext testContext, Map<String, String> config) {
        try {
            String testName = testContext.getName();
            //create, configure, and start a zookeeper resource
            String zkTempDirectory = fileSystemUtil.createPath("target", "zookeeper", testName);
            File zkDirectory = fileSystemUtil.recreateDirectory(zkTempDirectory);
            zkServer = new TestingServer(-1, zkDirectory, true);

            config.put("zookeeper.connect", zkServer.getConnectString());
            String logDir = config.get("log.dir");
            File logDirectory = fileSystemUtil.recreateDirectory(logDir);
            config.put("log.dir", logDirectory.getAbsolutePath());
            KafkaConfig kafkaConfig = new KafkaConfig(config);
            Option<String> threadNamePrefix = Option.apply(null);
            SystemTime$ time = kafka.utils.SystemTime$.MODULE$;
            server = new KafkaServer(kafkaConfig, time, threadNamePrefix);
            server.startup();

            int port = server.boundPort(SecurityProtocol.PLAINTEXT);
            Map<String, Object> producerConfig = new HashMap<>();
            producerConfig.put("bootstrap.servers", "localhost:" + port);
            producerConfig.put("acks", "all");
            producerConfig.put("retries", 0);
            producerConfig.put("batch.size", 16384);
            producerConfig.put("linger.ms", 1);
            producerConfig.put("buffer.memory", 33554432);
            producerConfig.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            producerConfig.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

            client = new KafkaProducer<>(producerConfig);

            return new ResourceInstanceBuilder<KafkaServer, KafkaProducer>()
                    .server(server, "kafkaServer")
                    .client(client, "kafkaProducer", Producer.class)
                    .build();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void stop() {
        try {
            client.close();
            server.shutdown();
            zkServer.close();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

}
