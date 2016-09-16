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
package org.springframework.cassandra.support;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.cassandra.core.ReactiveSessionFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.util.Assert;

import com.datastax.driver.core.exceptions.DriverException;

/**
 * Base class for {@link org.springframework.cassandra.core.ReactiveCqlTemplate} and other CQL-accessing DAO helpers,
 * defining common properties such as {@link org.springframework.cassandra.core.ReactiveSessionFactory} and exception
 * translator.
 * <p>
 * Not intended to be used directly. See {@link org.springframework.cassandra.core.ReactiveCqlTemplate}.
 *
 * @author Mark Paluch
 * @since 2.0
 * @see InitializingBean
 * @see org.springframework.cassandra.core.ReactiveSession
 */
public abstract class ReactiveCassandraAccessor implements InitializingBean {

	/** Logger available to subclasses */
	protected final Logger logger = LoggerFactory.getLogger(getClass());

	private PersistenceExceptionTranslator exceptionTranslator = new CassandraExceptionTranslator();

	private ReactiveSessionFactory sessionFactory;

	/**
	 * Sets the {@link ReactiveSessionFactory} to use.
	 * 
	 * @param sessionFactory must not be {@literal null}.
	 */
	public void setSessionFactory(ReactiveSessionFactory sessionFactory) {

		Assert.notNull(sessionFactory, "ReactiveSessionFactory must not be null");

		this.sessionFactory = sessionFactory;
	}

	/**
	 * Returns the configured {@link ReactiveSessionFactory}.
	 * 
	 * @return the configured {@link ReactiveSessionFactory}.
	 */
	public ReactiveSessionFactory getSessionFactory() {
		return sessionFactory;
	}

	/**
	 * Sets the exception translator used by this template to translate Cassandra specific exceptions into Spring DAO's
	 * Exception Hierarchy.
	 *
	 * @param exceptionTranslator exception translator to set; must not be {@literal null}.
	 * @see CassandraExceptionTranslator
	 * @see DataAccessException
	 */
	public void setExceptionTranslator(CassandraExceptionTranslator exceptionTranslator) {

		Assert.notNull(exceptionTranslator, "CassandraExceptionTranslator must not be null");

		this.exceptionTranslator = exceptionTranslator;
	}

	/**
	 * Returns the exception translator for this instance.
	 *
	 * @return the Cassandra exception translator.
	 * @see CassandraExceptionTranslator
	 */
	public PersistenceExceptionTranslator getExceptionTranslator() {
		return this.exceptionTranslator;
	}

	/**
	 * Ensures the Cassandra {@link ReactiveSessionFactory} and exception translator has been properly set.
	 */
	@Override
	public void afterPropertiesSet() {

		Assert.notNull(sessionFactory != null, "ReactiveSessionFactory must not be null");
		Assert.notNull(exceptionTranslator != null, "CassandraExceptionTranslator must not be null");
	}

	/**
	 * Attempts to translate the {@link Exception} into a Spring Data {@link Exception}.
	 *
	 * @param e the {@link Exception} to translate.
	 * @return the translated {@link RuntimeException}.
	 * @see <a href=
	 *      "http://docs.spring.io/spring/docs/current/spring-framework-reference/htmlsingle/#dao-exceptions">Consistent
	 *      exception hierarchy</a>
	 * @see DataAccessException
	 */
	protected DataAccessException translateExceptionIfPossible(DriverException ex) {

		Assert.notNull(ex, "DriverException must not be null");

		return getExceptionTranslator().translateExceptionIfPossible(ex);
	}
}
