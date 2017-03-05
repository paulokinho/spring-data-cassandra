/*
 * Copyright 2016-2017 the original author or authors.
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
package org.springframework.data.cassandra.test.integration.support;

import org.springframework.cassandra.core.cql.generator.CreateTableCqlGenerator;
import org.springframework.cassandra.core.keyspace.CreateTableSpecification;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.mapping.CassandraPersistentEntity;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;

/**
 * {@link SchemaTestUtils} is a collection of reflection-based utility methods for use in unit and integration testing
 * scenarios.
 * 
 * @author Mark Paluch
 */
public class SchemaTestUtils {

	/**
	 * Create a table for {@code entityClass} if it not exists.
	 * 
	 * @param entityClass must not be {@literal null}.
	 * @param operations must not be {@literal null}.
	 */
	public static void potentiallyCreateTableFor(Class<?> entityClass, CassandraOperations operations) {

		CassandraMappingContext mappingContext = operations.getConverter().getMappingContext();
		CassandraPersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(entityClass);
		Session session = operations.getSession();

		KeyspaceMetadata keyspace = session.getCluster().getMetadata().getKeyspace(session.getLoggedKeyspace());
		if (keyspace.getTable(persistentEntity.getTableName().toCql()) == null) {
			CreateTableSpecification tableSpecification = mappingContext.getCreateTableSpecificationFor(persistentEntity);
			operations.execute(new CreateTableCqlGenerator(tableSpecification).toCql());
		}
	}

	/**
	 * Truncate table for {@code entityClass}.
	 * 
	 * @param entityClass must not be {@literal null}.
	 * @param operations must not be {@literal null}.
	 */
	public static void truncate(Class<?> entityClass, CassandraOperations operations) {

		CassandraMappingContext mappingContext = operations.getConverter().getMappingContext();
		CassandraPersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(entityClass);

		operations.execute(QueryBuilder.truncate(persistentEntity.getTableName().toCql()));
	}
}
