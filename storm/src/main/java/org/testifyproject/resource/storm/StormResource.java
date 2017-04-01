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
package org.testifyproject.resource.storm;

import org.apache.storm.ILocalCluster;
import org.apache.storm.LocalCluster;
import org.apache.storm.utils.Utils;
import org.testifyproject.LocalResourceProvider;
import org.testifyproject.ResourceInstance;
import org.testifyproject.TestContext;
import org.testifyproject.core.ResourceInstanceBuilder;
import org.testifyproject.core.util.FileSystemUtil;
import static org.testifyproject.guava.common.base.StandardSystemProperty.USER_DIR;

/**
 * An implementation of LocalResourceProvider that provides a local storm cluster.
 *
 * @author saden
 */
public class StormResource implements LocalResourceProvider<Void, ILocalCluster, Void> {

    private final FileSystemUtil fileSystemUtil = FileSystemUtil.INSTANCE;

    private LocalCluster localCluster;

    @Override
    public Void configure(TestContext testContext) {
        return null;
    }

    @Override
    public ResourceInstance<ILocalCluster, Void> start(TestContext testContext, Void config) {
        String testName = testContext.getName();
        String localDirectory = fileSystemUtil.createPath("target", "storm", testName);
        fileSystemUtil.recreateDirectory(localDirectory);
        //XXX: we have to set the user dir property to insure local cluster
        //creates the log directories in target test folder.
        System.setProperty(USER_DIR.key(), localDirectory);

        localCluster = new LocalCluster();

        return ResourceInstanceBuilder.builder()
                .instance(localCluster, "StormLocalClusterServer", ILocalCluster.class)
                .build();
    }

    @Override
    public void stop() {
        Utils.sleep(2000);
        localCluster.shutdown();
    }

}
