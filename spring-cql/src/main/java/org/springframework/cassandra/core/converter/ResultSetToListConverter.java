/*
 * Copyright 2017 the original author or authors.
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
package org.springframework.cassandra.core.converter;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

/**
 * {@link Converter} from {@link ResultSet} to {@link Map}.
 *
 * @author Mark Paluch
 */
public class ResultSetToListConverter implements Converter<ResultSet, List<Map<String, Object>>> {

	protected Converter<Row, Map<String, Object>> rowConverter = new RowToMapConverter();

	public ResultSetToListConverter() {}

	public ResultSetToListConverter(Converter<Row, Map<String, Object>> rowConverter) {
		setRowConverter(rowConverter);
	}

	public Converter<Row, Map<String, Object>> getRowConverter() {
		return rowConverter;
	}

	public void setRowConverter(Converter<Row, Map<String, Object>> rowConverter) {

		Assert.notNull(rowConverter, "Converter must not be null");

		this.rowConverter = rowConverter;
	}

	@Override
	public List<Map<String, Object>> convert(ResultSet resultSet) {

		if (resultSet == null) {
			return null;
		}

		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		Iterator<Row> i = resultSet.iterator();
		while (i.hasNext()) {
			list.add(rowConverter.convert(i.next()));
		}

		return list;
	}
}
