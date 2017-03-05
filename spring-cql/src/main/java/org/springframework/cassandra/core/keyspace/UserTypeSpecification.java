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
package org.springframework.cassandra.core.keyspace;

import static org.springframework.cassandra.core.cql.CqlIdentifier.cqlId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.springframework.cassandra.core.cql.CqlIdentifier;

import com.datastax.driver.core.DataType;

/**
 * Builder class to support the construction of user type specifications that have columns. This class can also be used
 * as a standalone {@link UserTypeSpecification}.
 *
 * @author Fabio J. Mendes
 * @author Mark Paluch
 * @since 1.5
 * @see CqlIdentifier
 */
public class UserTypeSpecification<T extends UserTypeSpecification<T>> extends UserTypeNameSpecification<T> {

	private final List<FieldSpecification> fields = new ArrayList<FieldSpecification>();

	/**
	 * Adds the given field to the type.
	 *
	 * @param name must not be empty or {@literal null}.
	 * @param type The data type of the field.
	 * @return {@code this} specification.
	 */
	public T field(String name, DataType type) {
		return field(cqlId(name), type);
	}

	/**
	 * Adds an {@literal ADD} to the list of field changes.
	 *
	 * @param name must not {@literal null}.
	 * @param type must not be {@literal null}.
	 * @return {@code this} specification.
	 */
	@SuppressWarnings("unchecked")
	public T field(CqlIdentifier name, DataType type) {

		fields.add(new FieldSpecification().name(name).type(type));

		return (T) this;
	}

	/**
	 * @return an unmodifiable list of all fields.
	 */
	public List<FieldSpecification> getFields() {
		return Collections.unmodifiableList(fields);
	}
}
