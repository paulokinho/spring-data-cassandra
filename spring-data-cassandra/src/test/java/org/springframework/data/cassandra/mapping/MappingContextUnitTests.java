/*
 * Copyright 2016 the original author or authors.
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

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.springframework.cassandra.core.PrimaryKeyType;
import org.springframework.cassandra.core.cql.CqlIdentifier;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.cassandra.convert.CustomConversions;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.util.ClassTypeInformation;

/**
 * Unit tests for {@link BasicCassandraMappingContext}.
 *
 * @author Matthew T. Adams
 * @author Mark Paluch
 */
public class MappingContextUnitTests {

	BasicCassandraMappingContext mappingContext = new BasicCassandraMappingContext();

	@Test(expected = MappingException.class)
	public void testGetPersistentEntityOfTransientType() {
		mappingContext.getPersistentEntity(Transient.class);
	}

	private static class Transient {}

	@Test
	public void testGetExistingPersistentEntityHappyPath() {

		mappingContext.getPersistentEntity(X.class);

		assertThat(mappingContext.contains(X.class)).isTrue();
		assertThat(mappingContext.getExistingPersistentEntity(X.class)).isNotNull();
		assertThat(mappingContext.contains(Y.class)).isFalse();
	}

	@Table
	private static class X {
		@PrimaryKey String key;
	}

	@Table
	private static class Y {
		@PrimaryKey String key;
	}

	/**
	 * @see DATACASS-248
	 */
	@Test
	public void primaryKeyOnPropertyShouldWork() {

		CassandraPersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(PrimaryKeyOnProperty.class);

		CassandraPersistentProperty idProperty = persistentEntity.getIdProperty();

		assertThat(idProperty.getColumnName().toCql()).isEqualTo("foo");

		List<CqlIdentifier> columnNames = idProperty.getColumnNames();

		assertThat(columnNames).hasSize(1);
		assertThat(columnNames.get(0).toCql()).isEqualTo("foo");
	}

	@Table
	private static class PrimaryKeyOnProperty {

		String key;

		@PrimaryKey(value = "foo")
		public String getKey() {
			return key;
		}

		public void setKey(String key) {
			this.key = key;
		}
	}

	/**
	 * @see DATACASS-248
	 */
	@Test
	public void primaryKeyColumnsOnPropertyShouldWork() {

		CassandraPersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(PrimaryKeyColumnsOnProperty.class);

		assertThat(persistentEntity.isCompositePrimaryKey()).isFalse();

		CassandraPersistentProperty firstname = persistentEntity.getPersistentProperty("firstname");

		assertThat(firstname.isCompositePrimaryKey()).isFalse();
		assertThat(firstname.isPrimaryKeyColumn()).isTrue();
		assertThat(firstname.isPartitionKeyColumn()).isTrue();
		assertThat(firstname.getColumnName().toCql()).isEqualTo("firstname");

		CassandraPersistentProperty lastname = persistentEntity.getPersistentProperty("lastname");

		assertThat(lastname.isPrimaryKeyColumn()).isTrue();
		assertThat(lastname.isClusterKeyColumn()).isTrue();
		assertThat(lastname.getColumnName().toCql()).isEqualTo("mylastname");
	}

	@Table
	private static class PrimaryKeyColumnsOnProperty {

		String firstname;
		String lastname;

		@PrimaryKeyColumn(ordinal = 1, type = PrimaryKeyType.PARTITIONED)
		public String getFirstname() {
			return firstname;
		}

		public void setFirstname(String firstname) {
			this.firstname = firstname;
		}

		@PrimaryKeyColumn(name = "mylastname", ordinal = 2, type = PrimaryKeyType.CLUSTERED)
		public String getLastname() {
			return lastname;
		}

		public void setLastname(String lastname) {
			this.lastname = lastname;
		}
	}

	/**
	 * @see DATACASS-248
	 */
	@Test
	public void primaryKeyClassWithprimaryKeyColumnsOnPropertyShouldWork() {

		CassandraPersistentEntity<?> persistentEntity =
				mappingContext.getPersistentEntity(PrimaryKeyOnPropertyWithPrimaryKeyClass.class);

		CassandraPersistentEntity<?> primaryKeyClass =
				mappingContext.getPersistentEntity(CompositePrimaryKeyClassWithProperties.class);

		assertThat(persistentEntity.isCompositePrimaryKey()).isFalse();
		assertThat(persistentEntity.getPersistentProperty("key").isCompositePrimaryKey()).isTrue();

		assertThat(primaryKeyClass.isCompositePrimaryKey()).isTrue();
		assertThat(primaryKeyClass.getCompositePrimaryKeyProperties()).hasSize(2);

		CassandraPersistentProperty firstname = primaryKeyClass.getPersistentProperty("firstname");

		assertThat(firstname.isPrimaryKeyColumn()).isTrue();
		assertThat(firstname.isPartitionKeyColumn()).isTrue();
		assertThat(firstname.isClusterKeyColumn()).isFalse();
		assertThat(firstname.getColumnName().toCql()).isEqualTo("firstname");

		CassandraPersistentProperty lastname = primaryKeyClass.getPersistentProperty("lastname");

		assertThat(lastname.isPrimaryKeyColumn()).isTrue();
		assertThat(lastname.isPartitionKeyColumn()).isFalse();
		assertThat(lastname.isClusterKeyColumn()).isTrue();
		assertThat(lastname.getColumnName().toCql()).isEqualTo("mylastname");
	}

	@Table
	private static class PrimaryKeyOnPropertyWithPrimaryKeyClass {

		CompositePrimaryKeyClassWithProperties key;

		@PrimaryKey
		public CompositePrimaryKeyClassWithProperties getKey() {
			return key;
		}

		public void setKey(CompositePrimaryKeyClassWithProperties key) {
			this.key = key;
		}
	}

	@PrimaryKeyClass
	private static class CompositePrimaryKeyClassWithProperties implements Serializable {

		String firstname;
		String lastname;

		@PrimaryKeyColumn(ordinal = 1, type = PrimaryKeyType.PARTITIONED)
		public String getFirstname() {
			return firstname;
		}

		public void setFirstname(String firstname) {
			this.firstname = firstname;
		}

		@PrimaryKeyColumn(name = "mylastname", ordinal = 2, type = PrimaryKeyType.CLUSTERED)
		public String getLastname() {
			return lastname;
		}

		public void setLastname(String lastname) {
			this.lastname = lastname;
		}
	}

	/**
	 * @see DATACASS-296
	 */
	@Test
	public void shouldCreatePersistentEntityIfNoConversionRegistered() {

		mappingContext.setCustomConversions(new CustomConversions(Collections.EMPTY_LIST));
		assertThat(mappingContext.shouldCreatePersistentEntityFor(ClassTypeInformation.from(Human.class))).isTrue();
	}

	/**
	 * @see DATACASS-296
	 */
	@Test
	public void shouldNotCreateEntitiesForCustomConvertedTypes() {

		List<?> converters = Arrays.asList(new HumanToStringConverter());
		mappingContext.setCustomConversions(new CustomConversions(converters));

		assertThat(mappingContext.shouldCreatePersistentEntityFor(ClassTypeInformation.from(Human.class))).isFalse();
	}

	private static class Human {
	}

	private static class HumanToStringConverter implements Converter<Human, String> {

		@Override
		public String convert(Human source) {
			return "hello";
		}
	}
}
