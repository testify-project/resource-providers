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
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.testifyproject.LocalResourceInstance;
import org.testifyproject.LocalResourceProvider;
import org.testifyproject.TestContext;
import org.testifyproject.annotation.LocalResource;
import org.testifyproject.core.LocalResourceInstanceBuilder;
import org.testifyproject.core.util.FileSystemUtil;
import org.testifyproject.trait.PropertiesReader;

/**
 * An implementation of LocalResourceProvider that provides a local ZooKeeper
 * test server and client using Apache Curator.
 *
 * @author saden
 */
public class ZooKeeperResource implements LocalResourceProvider<Void, TestingServer, CuratorFramework> {

    private final FileSystemUtil fileSystemUtil = FileSystemUtil.INSTANCE;
    private TestingServer server;
    private CuratorFramework client;

    @Override
    public Void configure(TestContext testContext, LocalResource localResource, PropertiesReader configReader) {
        return null;
    }

    @Override
    public LocalResourceInstance<TestingServer, CuratorFramework> start(TestContext testContext,
            LocalResource localResource,
            Void config) throws Exception {
        String testName = testContext.getName();
        String tempDirectory = fileSystemUtil.createPath("target", "zookeeper", testName);
        File directory = fileSystemUtil.recreateDirectory(tempDirectory);
        server = new TestingServer(-1, directory, true);
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        client = CuratorFrameworkFactory.newClient(server.getConnectString(), retryPolicy);
        client.start();

        return LocalResourceInstanceBuilder.builder()
                .resource(server)
                .client(client, CuratorFramework.class)
                .build("zookeeper");
    }

    @Override
    public void stop(TestContext testContext, LocalResource localResource) throws Exception {
        client.close();
        server.close();
    }

}
