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

import org.springframework.cassandra.core.cql.CqlIdentifier;
import org.springframework.util.Assert;

/**
 * Abstract builder class to support the construction of user type specifications.
 *
 * @author Fabio J. Mendes
 * @author Mark Paluch
 * @param <T> Subtype of {@link UserTypeNameSpecification}.
 * @since 1.5
 * @see CqlIdentifier
 */
public abstract class UserTypeNameSpecification<T extends UserTypeNameSpecification<T>> {

	private CqlIdentifier name;

	/**
	 * Sets the type name.
	 *
	 * @param name must not be empty or {@literal null}.
	 * @return this
	 */
	public T name(String name) {
		return name(CqlIdentifier.cqlId(name));
	}

	/**
	 * Sets the type name.
	 *
	 * @param name must not be {@literal null}.
	 * @return this
	 */
	@SuppressWarnings("unchecked")
	public T name(CqlIdentifier name) {

		Assert.notNull(name, "Name must not be null");

		this.name = name;

		return (T) this;
	}

	/**
	 * @return the user type name.
	 */
	public CqlIdentifier getName() {
		return name;
	}
}
