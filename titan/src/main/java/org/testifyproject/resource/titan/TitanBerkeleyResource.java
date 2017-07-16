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
package org.testifyproject.resource.titan;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import static com.thinkaurelius.titan.diskstorage.configuration.BasicConfiguration.Restriction.NONE;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.backend.CommonsConfiguration;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.ROOT_NS;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.testifyproject.LocalResourceInstance;
import org.testifyproject.LocalResourceProvider;
import org.testifyproject.TestContext;
import org.testifyproject.annotation.LocalResource;
import org.testifyproject.core.LocalResourceInstanceBuilder;
import org.testifyproject.core.util.FileSystemUtil;
import org.testifyproject.trait.PropertiesReader;

/**
 * An implementation of LocalResourceProvider that provides a Berkeley DB backed
 * Titan graph.
 *
 * @author saden
 */
public class TitanBerkeleyResource implements LocalResourceProvider<CommonsConfiguration, TitanGraph, GraphTraversalSource> {

    private final FileSystemUtil fileSystemUtil = FileSystemUtil.INSTANCE;

    private TitanGraph server;
    private GraphTraversalSource client;

    @Override
    public CommonsConfiguration configure(TestContext testContext, LocalResource localResource, PropertiesReader configReader) {
        String testName = testContext.getName();
        String storageDirectory = fileSystemUtil.createPath("target", "elasticsearch", testName);

        CommonsConfiguration configuration = new CommonsConfiguration();
        configuration.set("storage.backend", "berkeleyje");
        configuration.set("storage.directory", storageDirectory);

        return configuration;
    }

    @Override
    public LocalResourceInstance<TitanGraph, GraphTraversalSource> start(TestContext testContext,
            LocalResource localResource,
            CommonsConfiguration config) throws Exception {
        String storageDirectory = config.get("storage.directory", String.class);
        fileSystemUtil.recreateDirectory(storageDirectory);

        ModifiableConfiguration configuration = new ModifiableConfiguration(ROOT_NS, config, NONE);
        server = TitanFactory.open(configuration);
        client = server.traversal();

        return LocalResourceInstanceBuilder.builder()
                .resource(server, TitanGraph.class)
                .client(client, GraphTraversalSource.class)
                .build("titan");
    }

    @Override
    public void stop(TestContext testContext,
            LocalResource localResource,
            LocalResourceInstance<TitanGraph, GraphTraversalSource> instance)
            throws Exception {
        server.close();
    }

}
