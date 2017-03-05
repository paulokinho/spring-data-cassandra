/*
 * Copyright 2013-2017 the original author or authors
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.config;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.cassandra.convert.CassandraConverter;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.util.Assert;

import com.datastax.driver.core.Session;

/**
 * Factory for configuring a {@link CassandraTemplate}.
 *
 * @author Matthew T. Adams
 * @author Mark Paluch
 */
public class CassandraTemplateFactoryBean implements FactoryBean<CassandraTemplate>, InitializingBean {

	protected Session session;

	protected CassandraConverter converter;

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	@Override
	public void afterPropertiesSet() throws Exception {

		Assert.notNull(session, "Session must not be null");
		Assert.notNull(converter, "Converter must not be null");
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.FactoryBean#getObject()
	 */
	@Override
	public CassandraTemplate getObject() throws Exception {
		return new CassandraTemplate(session, converter);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.FactoryBean#getObjectType()
	 */
	@Override
	public Class<CassandraTemplate> getObjectType() {
		return CassandraTemplate.class;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.FactoryBean#isSingleton()
	 */
	@Override
	public boolean isSingleton() {
		return true;
	}

	/**
	 * Sets the Cassandra {@link Session} to use. The {@link CassandraTemplate} will use the logged keyspace of the
	 * underlying {@link Session}. Don't change the keyspace using CQL but use multiple {@link Session} and
	 * {@link CassandraTemplate} beans.
	 * 
	 * @param session must not be {@literal null}.
	 */
	public void setSession(Session session) {

		Assert.notNull(session, "Session must not be null");

		this.session = session;
	}

	/**
	 * Set the {@link CassandraConverter} to use.
	 * 
	 * @param converter must not be {@literal null}.
	 */
	public void setConverter(CassandraConverter converter) {

		Assert.notNull(converter, "Converter must not be null");

		this.converter = converter;
	}
}
