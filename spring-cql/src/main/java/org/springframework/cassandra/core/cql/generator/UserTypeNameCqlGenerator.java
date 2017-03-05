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
package org.springframework.cassandra.core.cql.generator;

import org.springframework.cassandra.core.keyspace.UserTypeNameSpecification;
import org.springframework.util.Assert;

/**
 * Abstract class to support User type CQL generation.
 *
 * @author Fabio J. Mendes
 * @author Mark Paluch
 * @param <T> specification type
 * @since 1.5
 * @see UserTypeNameSpecification
 */
public abstract class UserTypeNameCqlGenerator<T extends UserTypeNameSpecification<T>> {

	public abstract StringBuilder toCql(StringBuilder cql);

	private UserTypeNameSpecification<T> specification;

	/**
	 * Creates a new {@link UserTypeNameCqlGenerator}.
	 * 
	 * @param specification must not be {@literal null}.
	 */
	public UserTypeNameCqlGenerator(UserTypeNameSpecification<T> specification) {
		setSpecification(specification);
	}

	/**
	 * Sets the {@link UserTypeNameSpecification}.
	 * 
	 * @param specification must not be {@literal null}.
	 */
	protected final void setSpecification(UserTypeNameSpecification<T> specification) {

		Assert.notNull(specification, "UserTypeNameSpecification must not be null");

		this.specification = specification;
	}

	@SuppressWarnings("unchecked")
	public T getSpecification() {
		return (T) specification;
	}

	/**
	 * Convenient synonymous method of {@link #getSpecification()}.
	 */
	protected T spec() {
		return getSpecification();
	}

	public String toCql() {
		return toCql(null).toString();
	}
}
