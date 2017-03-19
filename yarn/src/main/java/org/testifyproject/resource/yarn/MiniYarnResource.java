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

import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.testifyproject.ResourceInstance;
import org.testifyproject.ResourceProvider;
import org.testifyproject.TestContext;
import org.testifyproject.core.ResourceInstanceBuilder;
import org.testifyproject.core.util.FileSystemUtil;

/**
 * An implementation of ResourceProvider that provides a local mini YARN cluster
 * and a YARN client.
 *
 * @author saden
 */
public class MiniYarnResource implements ResourceProvider<YarnConfiguration, MiniYARNCluster, YarnClient> {

    private final FileSystemUtil fileSystemUtil = FileSystemUtil.INSTANCE;
    private MiniYARNCluster server;
    private YarnClient client;

    @Override
    public YarnConfiguration configure(TestContext testContext) {
        String testName = testContext.getName();
        String logDirectory = fileSystemUtil.createPath("target", "yarn", testName);

        YarnConfiguration configuration = new YarnConfiguration();
        configuration.set(YarnConfiguration.YARN_APP_CONTAINER_LOG_DIR, logDirectory);
        configuration.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 64);
        configuration.setClass(YarnConfiguration.RM_SCHEDULER, FifoScheduler.class, ResourceScheduler.class);

        return configuration;
    }

    @Override
    public ResourceInstance<MiniYARNCluster, YarnClient> start(TestContext testContext, YarnConfiguration config) {
        String logDirectory = config.get(YarnConfiguration.YARN_APP_CONTAINER_LOG_DIR);
        fileSystemUtil.recreateDirectory(logDirectory);
        server = new MiniYARNCluster(testContext.getName(), 1, 1, 1, 1, true);
        server.init(config);
        server.start();

        client = YarnClient.createYarnClient();
        client.init(server.getConfig());
        client.start();

        return new ResourceInstanceBuilder<MiniYARNCluster, YarnClient>()
                .server(server, "miniYarnResourceServer")
                .client(client, "miniYarnResourceClient")
                .build();
    }

    @Override
    public void stop() {
        client.stop();
        server.stop();
    }

}
