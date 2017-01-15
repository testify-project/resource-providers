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
package org.testify.resource.hsql;

import java.sql.Connection;
import javax.sql.DataSource;
import static org.assertj.core.api.Assertions.assertThat;
import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import org.testify.ResourceInstance;
import org.testify.TestContext;

/**
 *
 * @author saden
 */
public class InMemoryHSQLResourceTest {

    InMemoryHSQLResource cut;
    TestContext testContext;

    @Before
    public void init() {
        cut = new InMemoryHSQLResource();
        testContext = mock(TestContext.class);
        given(testContext.getName()).willReturn("test");

    }

    @After
    public void destroy() {
        cut.stop();
    }

    @Test
    public void configureAndStartRequiredResource() {
        JDBCDataSource config = cut.configure(testContext);
        assertThat(config).isNotNull();

        ResourceInstance<DataSource, Connection> resourceInstance = cut.start(testContext, config);
        assertThat(resourceInstance.getServer()).isNotNull();
        assertThat(resourceInstance.getClient()).isPresent();
    }

}
