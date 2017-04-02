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
package org.testifyproject.resource.hdfs;

import java.io.IOException;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.testifyproject.LocalResourceProvider;
import org.testifyproject.LocalResourceInstance;
import org.testifyproject.TestContext;
import org.testifyproject.core.LocalResourceInstanceBuilder;
import org.testifyproject.core.util.FileSystemUtil;

/**
 * An implementation of LocalResourceProvider that provides a local HDFS test cluster
 * server and file system client.
 *
 * @author saden
 */
public class MiniDFSResource implements LocalResourceProvider<HdfsConfiguration, MiniDFSCluster, DistributedFileSystem> {

    private final FileSystemUtil fileSystemUtil = FileSystemUtil.INSTANCE;
    private MiniDFSCluster hdfsCluster;
    private DistributedFileSystem fileSystem;

    @Override
    public HdfsConfiguration configure(TestContext testContext) {
        String testName = testContext.getName();
        String hdfsDirectory = fileSystemUtil.createPath("target", "hdfs", testName);
        HdfsConfiguration configuration = new HdfsConfiguration();
        configuration.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, hdfsDirectory);

        return configuration;
    }

    @Override
    public LocalResourceInstance<MiniDFSCluster, DistributedFileSystem> start(TestContext testContext, HdfsConfiguration config) {
        try {
            String hdfsDirectory = config.get(MiniDFSCluster.HDFS_MINIDFS_BASEDIR);
            fileSystemUtil.recreateDirectory(hdfsDirectory);
            config.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, hdfsDirectory);
            MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(config);
            hdfsCluster = builder.build();
            fileSystem = hdfsCluster.getFileSystem();

            return LocalResourceInstanceBuilder.builder()
                    .resource(hdfsCluster, "hdfsMiniCluster")
                    .client(fileSystem, "hdfsFileSystem")
                    .build();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void stop() {
        try {
            fileSystem.close();
            hdfsCluster.shutdown();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

}
