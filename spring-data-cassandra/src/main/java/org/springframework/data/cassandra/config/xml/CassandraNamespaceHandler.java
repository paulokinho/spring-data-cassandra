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
package org.springframework.data.cassandra.config.xml;

import org.springframework.beans.factory.xml.NamespaceHandlerSupport;
import org.springframework.data.cassandra.repository.config.CassandraRepositoryConfigurationExtension;
import org.springframework.data.repository.config.RepositoryBeanDefinitionParser;

/**
 * Namespace handler for spring-data-cassandra.
 *
 * @author Alex Shvid
 * @author Matthew T. Adams
 * @author Mark Paluch
 */
public class CassandraNamespaceHandler extends NamespaceHandlerSupport {

	/* (non-Javadoc)
	 * @see org.springframework.beans.factory.xml.NamespaceHandler#init()
	 */
	@Override
	public void init() {

		registerBeanDefinitionParser("cluster", new CassandraClusterParser());
		registerBeanDefinitionParser("session", new CassandraSessionParser());
		registerBeanDefinitionParser("template", new CassandraTemplateParser());
		registerBeanDefinitionParser("converter", new CassandraMappingConverterParser());
		registerBeanDefinitionParser("mapping", new CassandraMappingContextParser());
		registerBeanDefinitionParser("repositories",
				new RepositoryBeanDefinitionParser(new CassandraRepositoryConfigurationExtension()));
	}
}
