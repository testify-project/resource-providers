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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
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
public class MiniDFSResourceTest {

    @Sut
    MiniDFSResource sut;

    @Test
    public void callToStartResourceShouldReturnRequiredResource() throws IOException,
            Exception {
        TestContext testContext = mock(TestContext.class);
        LocalResource localResource = mock(LocalResource.class, Answers.RETURNS_MOCKS);
        PropertiesReader configReader = mock(PropertiesReader.class);

        given(testContext.getName()).willReturn("test");

        HdfsConfiguration config = sut.configure(testContext, localResource, configReader);
        assertThat(config).isNotNull();

        LocalResourceInstance<MiniDFSCluster, DistributedFileSystem> result = sut.start(
                testContext, localResource, config);

        assertThat(result).isNotNull();
        assertThat(result.getClient()).isPresent();
        assertThat(result.getResource()).isNotNull();

        FileSystem fileSystem = result.getClient().get().getValue();
        short replicationFactor = 2;
        String fileName = "/testFile";
        Path path = new Path(fileName);
        try (FSDataOutputStream stream = fileSystem.create(path, replicationFactor)) {
            stream.writeUTF("Hello!");
        }

        FileStatus status = fileSystem.getFileStatus(path);
        assertThat(status).isNotNull();

        sut.stop(testContext, localResource, result);
    }

}
