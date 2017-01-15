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
package org.testify.resource.storm;

import org.apache.storm.Config;
import static org.apache.storm.Config.STORM_LOCAL_DIR;
import org.apache.storm.ILocalCluster;
import org.apache.storm.LocalCluster;
import org.testify.ResourceInstance;
import org.testify.ResourceProvider;
import org.testify.TestContext;
import org.testify.core.impl.ResourceInstanceBuilder;
import org.testify.core.util.FileSystemUtil;

/**
 * An implementation of ResourceProvider that provides a local storm cluster.
 *
 * @author saden
 */
public class StormResource implements ResourceProvider<Config, LocalCluster, Void> {

    private final FileSystemUtil fileSystemUtil = FileSystemUtil.INSTANCE;

    private LocalCluster localCluster;

    @Override
    public Config configure(TestContext testContext) {
        String testName = testContext.getName();
        String localDirectory = fileSystemUtil.createPath("target", "storm", testName);

        Config config = new Config();
        config.put(STORM_LOCAL_DIR, localDirectory);

        return config;
    }

    @Override
    public ResourceInstance<LocalCluster, Void> start(TestContext testContext, Config config) {
        String localDirectory = (String) config.get(STORM_LOCAL_DIR);
        fileSystemUtil.recreateDirectory(localDirectory);
        localCluster = new LocalCluster();

        return new ResourceInstanceBuilder<LocalCluster, Void>()
                .server(localCluster, ILocalCluster.class)
                .build();
    }

    @Override
    public void stop() {
        localCluster.shutdown();
    }

}
