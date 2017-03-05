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
package org.springframework.data.cassandra.mapping;

import static org.assertj.core.api.Assertions.*;

import org.junit.Before;
import org.junit.Test;
import org.springframework.cassandra.core.Ordering;
import org.springframework.cassandra.core.PrimaryKeyType;
import org.springframework.data.annotation.Id;
import org.springframework.data.mapping.model.MappingException;

/**
 * Unit tests for {@link org.springframework.data.cassandra.mapping.BasicCassandraPersistentEntityMetadataVerifier}
 * through {@link CassandraMappingContext}
 *
 * @author David Webb
 * @author Mark Paluch
 */
public class BasicCassandraPersistentEntityMetadataVerifierUnitTests {

	private BasicCassandraPersistentEntityMetadataVerifier verifier = new BasicCassandraPersistentEntityMetadataVerifier();
	private BasicCassandraMappingContext context = new BasicCassandraMappingContext();

	@Before
	public void setUp() throws Exception {
		context.setVerifier(new NoOpVerifier());
	}

	@Test // DATACASS-258
	public void shouldAllowInterfaceTypes() {
		verifier.verify(getEntity(MyInterface.class));
	}

	@Test // DATACASS-258
	public void testPrimaryKeyClass() {
		verifier.verify(getEntity(Animal.class));
	}

	@Test // DATACASS-258
	public void testNonPrimaryKeyClass() {
		verifier.verify(getEntity(Person.class));
	}

	@Test // DATACASS-258
	public void testNonPersistentType() {
		verifier.verify(getEntity(NonPersistentClass.class));
	}

	@Test // DATACASS-258
	public void shouldFailWithPersistentAndPrimaryKeyClassAnnotations() {

		try {
			verifier.verify(getEntity(TooManyAnnotations.class));
			fail("Missing MappingException");
		} catch (MappingException e) {
			assertThat(e).hasMessageContaining("Entity cannot be of type @Table and @PrimaryKeyClass");
		}
	}

	@Test // DATACASS-258
	public void shouldFailWithoutPartitionKey() {

		try {
			verifier.verify(getEntity(NoPartitionKey.class));
			fail("Missing MappingException");
		} catch (MappingException e) {
			assertThat(e)
					.hasMessageContaining("At least one of the @PrimaryKeyColumn annotations must have a type of PARTITIONED");
		}
	}

	@Test // DATACASS-258
	public void shouldFailWithoutPrimaryKey() {

		try {
			verifier.verify(getEntity(NoPrimaryKey.class));
			fail("Missing MappingException");
		} catch (MappingException e) {
			assertThat(e).hasMessageContaining("@Table types must have only one primary attribute, if any; Found 0");
		}
	}

	@Test // DATACASS-258
	public void testPkAndPkc() {

		try {
			verifier.verify(getEntity(PrimaryKeyAndPrimaryKeyColumn.class));
			fail("Missing MappingException");
		} catch (MappingException e) {
			assertThat(e).hasMessageContaining("@Table types must not define both @Id and @PrimaryKeyColumn properties");
		}
	}

	private CassandraPersistentEntity<?> getEntity(Class<?> entityClass) {
		return context.getPersistentEntity(entityClass);
	}

	interface MyInterface {}

	static class NonPersistentClass {

		@Id String id;

		String foo;
		String bar;
	}

	@Table
	static class Person {

		@Id String id;

		String firstName;
		String lastName;
	}

	@Table
	static class Animal {

		@PrimaryKey AnimalPK key;
		String name;
	}

	@PrimaryKeyClass
	static class AnimalPK {

		@PrimaryKeyColumn(ordinal = 0, type = PrimaryKeyType.PARTITIONED) String species;
		@PrimaryKeyColumn(ordinal = 1, type = PrimaryKeyType.PARTITIONED) String breed;
		@PrimaryKeyColumn(ordinal = 2, type = PrimaryKeyType.CLUSTERED, ordering = Ordering.DESCENDING) String color;
	}

	@Table
	static class EntityWithComplexTypePrimaryKey {

		@PrimaryKeyColumn(ordinal = 0, type = PrimaryKeyType.PARTITIONED) Object species;
	}

	@Table
	@PrimaryKeyClass
	static class TooManyAnnotations {}

	@Table
	static class NoPartitionKey {

		@PrimaryKeyColumn(ordinal = 0) String key;
	}

	@Table
	static class NoPrimaryKey {}

	@Table
	static class PrimaryKeyAndPrimaryKeyColumn {

		@PrimaryKey String primaryKey;
		@PrimaryKeyColumn(ordinal = 0) String primaryKeyColumn;
	}

	@Table
	static class OnePrimaryKeyColumn {

		@PrimaryKeyColumn(type = PrimaryKeyType.PARTITIONED, ordinal = 0) String pk;
	}

	@Table
	static class MultiplePrimaryKeyColumns {

		@PrimaryKeyColumn(type = PrimaryKeyType.PARTITIONED, ordinal = 0) String pk0;
		@PrimaryKeyColumn(ordinal = 1) String pk1;
	}

	private static class NoOpVerifier implements CassandraPersistentEntityMetadataVerifier {

		@Override
		public void verify(CassandraPersistentEntity<?> entity) throws MappingException {}
	}
}
