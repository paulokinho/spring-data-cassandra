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

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.Field;
import java.util.Date;

import org.junit.Test;
import org.springframework.cassandra.core.cql.CqlIdentifier;
import org.springframework.core.annotation.AliasFor;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.util.ReflectionUtils;

import com.datastax.driver.core.DataType.Name;

/**
 * Unit tests for {@link BasicCassandraPersistentProperty}.
 *
 * @author Alex Shvid
 * @author Mark Paluch
 */
public class BasicCassandraPersistentPropertyUnitTests {

	@Test
	public void usesAnnotatedColumnName() {
		assertThat(getPropertyFor(Timeline.class, "text").getColumnName().toCql()).isEqualTo("message");
	}

	@Test
	public void checksIdProperty() {

		CassandraPersistentProperty property = getPropertyFor(Timeline.class, "id");

		assertThat(property.isIdProperty()).isTrue();
	}

	@Test
	public void returnsPropertyNameForUnannotatedProperty() {
		assertThat(getPropertyFor(Timeline.class, "time").getColumnName().toCql()).isEqualTo("time");
	}

	@Test // DATACASS-259
	public void shouldConsiderComposedColumnAnnotation() {

		CassandraPersistentProperty persistentProperty = getPropertyFor(TypeWithComposedColumnAnnotation.class, "column");

		assertThat(persistentProperty.getColumnName()).isEqualTo(CqlIdentifier.cqlId("mycolumn", true));
	}

	@Test // DATACASS-259
	public void shouldConsiderComposedPrimaryKeyAnnotation() {

		CassandraPersistentProperty persistentProperty = getPropertyFor(TypeWithComposedPrimaryKeyAnnotation.class,
				"column");

		assertThat(persistentProperty.getColumnName()).isEqualTo(CqlIdentifier.cqlId("primary-key", true));
		assertThat(persistentProperty.isIdProperty()).isTrue();
	}

	@Test // DATACASS-259
	public void shouldConsiderComposedPrimaryKeyColumnAnnotation() {

		CassandraPersistentProperty persistentProperty = getPropertyFor(TypeWithComposedPrimaryKeyColumnAnnotation.class,
				"column");

		assertThat(persistentProperty.getColumnName()).isEqualTo(CqlIdentifier.cqlId("mycolumn", true));
		assertThat(persistentProperty.isPrimaryKeyColumn()).isTrue();
	}

	@Test // DATACASS-259
	public void shouldConsiderComposedCassandraTypeAnnotation() {

		CassandraPersistentProperty persistentProperty = getPropertyFor(TypeWithComposedCassandraTypeAnnotation.class,
				"column");

		assertThat(persistentProperty.getDataType().getName()).isEqualTo(Name.COUNTER);
		assertThat(persistentProperty.findAnnotation(CassandraType.class)).isNotNull();
	}

	private CassandraPersistentProperty getPropertyFor(Class<?> type, String fieldName) {

		Field field = ReflectionUtils.findField(type, fieldName);

		return new BasicCassandraPersistentProperty(field, null, getEntity(type), new CassandraSimpleTypeHolder());
	}

	private <T> BasicCassandraPersistentEntity<T> getEntity(Class<T> type) {
		return new BasicCassandraPersistentEntity<T>(ClassTypeInformation.from(type));
	}

	static class Timeline {

		@PrimaryKey String id;

		Date time;

		@Column("message") String text;
	}

	@Retention(RetentionPolicy.RUNTIME)
	@Column(forceQuote = true)
	@interface ComposedColumnAnnotation {

		@AliasFor(annotation = Column.class)
		String value();
	}

	@Retention(RetentionPolicy.RUNTIME)
	@PrimaryKey(forceQuote = true)
	@interface ComposedPrimaryKeyAnnotation {

		@AliasFor(annotation = PrimaryKey.class)
		String value() default "primary-key";
	}

	@Retention(RetentionPolicy.RUNTIME)
	@PrimaryKeyColumn(forceQuote = true)
	@interface ComposedPrimaryKeyColumnAnnotation {

		@AliasFor(annotation = PrimaryKeyColumn.class)
		String value();

		@AliasFor(annotation = PrimaryKeyColumn.class)
		int ordinal() default 42;
	}

	@Retention(RetentionPolicy.RUNTIME)
	@CassandraType(type = Name.COUNTER)
	@interface ComposedCassandraTypeAnnotation {
	}

	static class TypeWithComposedColumnAnnotation {
		@ComposedColumnAnnotation("mycolumn") String column;
	}

	static class TypeWithComposedPrimaryKeyAnnotation {
		@ComposedPrimaryKeyAnnotation String column;
	}

	static class TypeWithComposedPrimaryKeyColumnAnnotation {
		@ComposedPrimaryKeyColumnAnnotation("mycolumn") String column;
	}

	static class TypeWithComposedCassandraTypeAnnotation {
		@ComposedCassandraTypeAnnotation String column;
	}
}
