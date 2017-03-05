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
package org.springframework.cassandra.test.integration.core.cql.generator;

import static org.springframework.cassandra.test.integration.core.cql.generator.CqlTableSpecificationAssertions.*;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cassandra.core.cql.generator.CreateTableCqlGeneratorUnitTests;
import org.springframework.cassandra.core.cql.generator.DropTableCqlGenerator;
import org.springframework.cassandra.core.cql.generator.DropTableCqlGeneratorUnitTests;
import org.springframework.cassandra.core.keyspace.DropTableSpecification;
import org.springframework.cassandra.test.integration.AbstractKeyspaceCreatingIntegrationTest;

/**
 * Test CREATE TABLE / ALTER TABLE / DROP TABLE
 *
 * @author David Webb
 * @author Oliver Gierke
 * @author Mark Paluch
 */
public class TableLifecycleIntegrationTests extends AbstractKeyspaceCreatingIntegrationTest {

	private final static Logger log = LoggerFactory.getLogger(TableLifecycleIntegrationTests.class);

	CreateTableCqlGeneratorUnitTests.MultipleOptionsTest createTableTest = new CreateTableCqlGeneratorUnitTests.MultipleOptionsTest();

	@Before
	public void setUp() throws Exception {
		execute("cassandraOperationsTest-cql-dataload.cql", this.keyspace);
	}

	@Test
	public void dropIsSuccessful() {

		createTableTest.prepare();

		log.info(createTableTest.cql);

		session.execute(createTableTest.cql);

		assertTable(createTableTest.specification, keyspace, session);

		DropTableTest dropTest = new DropTableTest();
		dropTest.prepare();

		log.info(dropTest.cql);

		session.execute(dropTest.cql);

		assertNoTable(dropTest.specification, keyspace, session);
	}

	public class DropTableTest extends DropTableCqlGeneratorUnitTests.DropTableTest {

		@Override
		public DropTableSpecification specification() {
			return DropTableSpecification.dropTable().name(createTableTest.specification.getName());
		}

		@Override
		public DropTableCqlGenerator generator() {
			return new DropTableCqlGenerator(specification);
		}
	}
}
