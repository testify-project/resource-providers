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
package org.testify.resource.elasticsearch;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.testify.ResourceInstance;
import org.testify.ResourceProvider;
import org.testify.TestContext;
import org.testify.core.impl.ResourceInstanceBuilder;
import org.testify.core.util.FileSystemUtil;

/**
 * An implementation of ResourceProvider that provides a local Elasticsearch
 * node and client.
 *
 * @author saden
 */
public class ElasticsearchResource implements ResourceProvider<Settings.Builder, Node, Client> {

    private final FileSystemUtil fileSystemUtil = FileSystemUtil.INSTANCE;

    private Node node;
    private Client client;

    @Override
    public Settings.Builder configure(TestContext testContext) {
        String testName = testContext.getName();
        String pathHome = fileSystemUtil.createPath("target", "elasticsearch", testName);

        return Settings.builder()
                .put("node.name", testContext.getName())
                .put("path.home", pathHome);
    }

    @Override
    public ResourceInstance<Node, Client> start(TestContext testContext, Settings.Builder config) {
        String pathHome = config.get("path.home");
        fileSystemUtil.recreateDirectory(pathHome);

        node = NodeBuilder.nodeBuilder()
                .settings(config)
                .clusterName(testContext.getName())
                .data(true)
                .local(true)
                .node();

        node.start();
        client = node.client();

        return new ResourceInstanceBuilder<Node, Client>()
                .server(node)
                .client(client, Client.class)
                .build();
    }

    @Override
    public void stop() {
        client.close();
        node.close();
    }

}
