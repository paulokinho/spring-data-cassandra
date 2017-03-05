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
package org.springframework.data.cassandra;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.util.Assert;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;

/**
 * Utility to mock a Cassandra {@link Row}.
 * 
 * @author Mark Paluch
 */
public class RowMockUtil {

	/**
	 * Creates a new {@link Row} mock using the given {@code columns}. Each column carries a name, value and data type so
	 * users of {@link Row} can use most of the methods.
	 * 
	 * @param columns
	 * @return
	 */
	public static Row newRowMock(final Column... columns) {

		Assert.notNull(columns, "Columns must not be null");

		Row rowMock = mock(Row.class);
		ColumnDefinitions columnDefinitionsMock = mock(ColumnDefinitions.class);

		when(rowMock.getColumnDefinitions()).thenReturn(columnDefinitionsMock);

		when(columnDefinitionsMock.contains(anyString())).thenAnswer(new Answer<Boolean>() {
			@Override
			public Boolean answer(InvocationOnMock invocation) throws Throwable {

				for (Column column : columns) {
					if (column.name.equalsIgnoreCase((String) invocation.getArguments()[0])) {
						return true;
					}
				}

				return false;
			}
		});

		when(columnDefinitionsMock.getIndexOf(anyString())).thenAnswer(new Answer<Integer>() {
			@Override
			public Integer answer(InvocationOnMock invocation) throws Throwable {

				int counter = 0;
				for (Column column : columns) {
					if (column.name.equalsIgnoreCase((String) invocation.getArguments()[0])) {
						return counter;
					}
					counter++;
				}

				return -1;
			}
		});

		when(columnDefinitionsMock.getType(anyString())).thenAnswer(new Answer<DataType>() {
			@Override
			public DataType answer(InvocationOnMock invocation) throws Throwable {

				for (Column column : columns) {
					if (column.name.equalsIgnoreCase((String) invocation.getArguments()[0])) {
						return column.type;
					}
				}

				return null;
			}
		});

		when(columnDefinitionsMock.getType(anyInt())).thenAnswer(new Answer<DataType>() {
			@Override
			public DataType answer(InvocationOnMock invocation) throws Throwable {
				return columns[(Integer) invocation.getArguments()[0]].type;
			}
		});

		when(rowMock.getObject(anyInt())).thenAnswer(new Answer<Object>() {
			@Override
			public Object answer(InvocationOnMock invocation) throws Throwable {
				return columns[(Integer) invocation.getArguments()[0]].value;
			}
		});

		when(rowMock.getObject(anyString())).thenAnswer(new Answer<Object>() {
			@Override
			public Object answer(InvocationOnMock invocation) throws Throwable {

				for (Column column : columns) {
					if (column.name.equalsIgnoreCase((String) invocation.getArguments()[0])) {
						return column.value;
					}
				}

				return null;
			}
		});

		return rowMock;
	}

	/**
	 * Creates a new {@link Column} to be used with {@link RowMockUtil#newRowMock(Column...)}.
	 * 
	 * @param name must not be empty or {@link null}.
	 * @param value can be {@literal null}.
	 * @param type must not be {@literal null}.
	 * @return
	 */
	public static Column column(String name, Object value, DataType type) {

		Assert.hasText(name, "Name must not be empty");
		Assert.notNull(type, "DataType must not be null");

		return new Column(name, value, type);
	}

	public static class Column {

		private final String name;
		private final Object value;
		private final DataType type;

		Column(String name, Object value, DataType type) {
			this.name = name;
			this.value = value;
			this.type = type;
		}
	}

}
