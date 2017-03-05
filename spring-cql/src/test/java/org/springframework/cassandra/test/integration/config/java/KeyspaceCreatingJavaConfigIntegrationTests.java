/*
 * Copyright 2013-2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.cassandra.test.integration.config.java;

import static org.assertj.core.api.Assertions.*;

import org.junit.Test;
import org.springframework.cassandra.test.integration.config.IntegrationTestUtils;
import org.springframework.test.context.ContextConfiguration;

/**
 * @author Matthew T. Adams
 */
@ContextConfiguration(classes = KeyspaceCreatingJavaConfig.class)
public class KeyspaceCreatingJavaConfigIntegrationTests extends AbstractIntegrationTest {

	@Test
	public void test() {
		assertThat(session).isNotNull();
		IntegrationTestUtils.assertKeyspaceExists(KeyspaceCreatingJavaConfig.KEYSPACE_NAME, session);

		session.execute("DROP KEYSPACE " + KeyspaceCreatingJavaConfig.KEYSPACE_NAME + ";");
	}
}
