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
package org.springframework.data.cassandra.repository.query;

import java.util.Arrays;
import java.util.Iterator;

import org.springframework.data.cassandra.convert.CassandraConverter;
import org.springframework.data.cassandra.mapping.CassandraType;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.repository.query.ParameterAccessor;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.DataType;

/**
 * Simple {@link ParameterAccessor} that returns the given parameters unfiltered.
 * 
 * @author Mark Paluch
 */
class StubParameterAccessor implements CassandraParameterAccessor {

	private final Object[] values;

	/**
	 * Creates a new {@link ConvertingParameterAccessor} backed by a {@link StubParameterAccessor} simply returning the
	 * given parameters converted but unfiltered.
	 *
	 * @param converter
	 * @param parameters
	 * @return
	 */
	public static ConvertingParameterAccessor getAccessor(CassandraConverter converter, Object... parameters) {
		return new ConvertingParameterAccessor(converter, new StubParameterAccessor(parameters));
	}

	@SuppressWarnings("unchecked")
	public StubParameterAccessor(Object... values) {
		this.values = values;
	}

	@Override
	public DataType getDataType(int index) {
		return CodecRegistry.DEFAULT_INSTANCE.codecFor(values[index]).getCqlType();
	}

	@Override
	public Class<?> getParameterType(int index) {
		return values[index].getClass();
	}

	@Override
	public Pageable getPageable() {
		return null;
	}

	@Override
	public Sort getSort() {
		return null;
	}

	@Override
	public Class<?> getDynamicProjection() {
		return null;
	}

	@Override
	public Object getBindableValue(int index) {
		return values[index];
	}

	@Override
	public Object[] getValues() {
		return new Object[0];
	}

	@Override
	public boolean hasBindableNullValue() {
		return false;
	}

	@Override
	public Iterator<Object> iterator() {
		return Arrays.asList(values).iterator();
	}

	@Override
	public CassandraType findCassandraType(int index) {
		return null;
	}
}
