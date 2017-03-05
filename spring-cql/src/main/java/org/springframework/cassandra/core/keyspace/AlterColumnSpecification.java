/*
 * Copyright 2013-2014 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.cassandra.core.keyspace;

import org.springframework.cassandra.core.cql.CqlIdentifier;

import com.datastax.driver.core.DataType;

/**
 * Value object for altering a column.
 * 
 * @author Matthew T. Adams
 * @author Mark Paluch
 * @see CqlIdentifier
 */
public class AlterColumnSpecification extends ColumnTypeChangeSpecification {

	public AlterColumnSpecification(String name, DataType type) {
		super(name, type);
	}

	public AlterColumnSpecification(CqlIdentifier name, DataType type) {
		super(name, type);
	}
}
