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
package org.testifyproject.resource.zookeeper;

import java.io.File;
import java.io.IOException;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.testifyproject.LocalResourceProvider;
import org.testifyproject.ResourceInstance;
import org.testifyproject.TestContext;
import org.testifyproject.core.ResourceInstanceBuilder;
import org.testifyproject.core.util.FileSystemUtil;

/**
 * An implementation of LocalResourceProvider that provides a local ZooKeeper test
 * server and client using Apache Curator.
 *
 * @author saden
 */
public class ZooKeeperResource implements LocalResourceProvider<Void, TestingServer, CuratorFramework> {

    private final FileSystemUtil fileSystemUtil = FileSystemUtil.INSTANCE;
    private TestingServer server;
    private CuratorFramework client;

    @Override
    public Void configure(TestContext testContext) {
        return null;
    }

    @Override
    public ResourceInstance<TestingServer, CuratorFramework> start(TestContext testContext, Void config) {
        try {
            String testName = testContext.getName();
            String tempDirectory = fileSystemUtil.createPath("target", "zookeeper", testName);
            File directory = fileSystemUtil.recreateDirectory(tempDirectory);
            server = new TestingServer(-1, directory, true);
            RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
            client = CuratorFrameworkFactory.newClient(server.getConnectString(), retryPolicy);
            client.start();

            return ResourceInstanceBuilder.builder()
                    .resource(server, "zookeeperServer")
                    .client(client, "zookeeperClient", CuratorFramework.class)
                    .build();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void stop() {
        try {
            client.close();
            server.close();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

}
