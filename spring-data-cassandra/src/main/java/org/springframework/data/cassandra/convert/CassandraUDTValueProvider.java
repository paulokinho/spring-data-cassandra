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
package org.springframework.data.cassandra.convert;

import org.springframework.data.cassandra.mapping.CassandraPersistentProperty;
import org.springframework.data.mapping.model.DefaultSpELExpressionEvaluator;
import org.springframework.data.mapping.model.SpELExpressionEvaluator;
import org.springframework.util.Assert;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.UDTValue;

/**
 * {@link CassandraValueProvider} to read property values from a {@link UDTValue}.
 *
 * @author Mark Paluch
 * @since 1.5
 */
public class CassandraUDTValueProvider implements CassandraValueProvider {

	private final UDTValue udtValue;
	private final CodecRegistry codecRegistry;
	private final SpELExpressionEvaluator evaluator;

	/**
	 * Creates a new {@link CassandraUDTValueProvider} with the given {@link UDTValue} and
	 * {@link DefaultSpELExpressionEvaluator}.
	 *
	 * @param udtValue must not be {@literal null}.
	 * @param codecRegistry must not be {@literal null}.
	 * @param evaluator must not be {@literal null}.
	 */
	public CassandraUDTValueProvider(UDTValue udtValue, CodecRegistry codecRegistry,
			DefaultSpELExpressionEvaluator evaluator) {

		Assert.notNull(udtValue, "UDTValue must not be null");
		Assert.notNull(codecRegistry, "CodecRegistry must not be null");
		Assert.notNull(evaluator, "SpELExpressionEvaluator must not be null");

		this.udtValue = udtValue;
		this.codecRegistry = codecRegistry;
		this.evaluator = evaluator;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mapping.model.PropertyValueProvider#getPropertyValue(org.springframework.data.mapping.PersistentProperty)
	 */
	@SuppressWarnings("unchecked")
	public Object getPropertyValue(CassandraPersistentProperty property) {

		String expression = property.getSpelExpression();

		if (expression != null) {
			return evaluator.evaluate(expression);
		}

		String name = property.getColumnName().toCql();
		DataType fieldType = udtValue.getType().getFieldType(name);

		return udtValue.get(name, codecRegistry.codecFor(fieldType));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.convert.CassandraValueProvider#hasProperty(org.springframework.data.cassandra.mapping.CassandraPersistentProperty)
	 */
	@Override
	public boolean hasProperty(CassandraPersistentProperty property) {
		return udtValue.getType().contains(property.getColumnName().toCql());
	}
}
