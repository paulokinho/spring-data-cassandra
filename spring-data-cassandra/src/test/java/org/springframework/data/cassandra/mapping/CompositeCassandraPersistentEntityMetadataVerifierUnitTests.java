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
import static org.assertj.core.api.Fail.fail;

import org.junit.Before;
import org.junit.Test;
import org.springframework.cassandra.core.Ordering;
import org.springframework.cassandra.core.PrimaryKeyType;
import org.springframework.data.annotation.Id;
import org.springframework.data.mapping.model.MappingException;

/**
 * Unit tests for {@link CompositeCassandraPersistentEntityMetadataVerifier}.
 * 
 * @author Mark Paluch
 */
public class CompositeCassandraPersistentEntityMetadataVerifierUnitTests {

	private CompositeCassandraPersistentEntityMetadataVerifier verifier = new CompositeCassandraPersistentEntityMetadataVerifier();
	private BasicCassandraMappingContext context = new BasicCassandraMappingContext();

	@Before
	public void setUp() throws Exception {
		context.setVerifier(verifier);
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

	@Test // DATACASS-258, DATACASS-359
	public void shouldNotFailWithNonPersistentClasses() {
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
	@PrimaryKeyClass
	static class TooManyAnnotations {}

	@Table
	static class Person {

		@Id String id;

		String firstName;
		String lastName;
	}

	@Table
	static class Animal {

		@PrimaryKey AnimalPK key;
		private String name;
	}

	@PrimaryKeyClass
	static class AnimalPK {

		@PrimaryKeyColumn(ordinal = 0, type = PrimaryKeyType.PARTITIONED) String species;
		@PrimaryKeyColumn(ordinal = 1, type = PrimaryKeyType.PARTITIONED) String breed;
		@PrimaryKeyColumn(ordinal = 2, type = PrimaryKeyType.CLUSTERED, ordering = Ordering.DESCENDING) String color;
	}
}
