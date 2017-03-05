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
package org.springframework.data.cassandra.domain;

import lombok.Data;

import org.springframework.cassandra.core.PrimaryKeyType;
import org.springframework.data.cassandra.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.mapping.Table;
import org.springframework.data.cassandra.repository.MapId;
import org.springframework.data.cassandra.repository.MapIdentifiable;
import org.springframework.data.cassandra.repository.support.BasicMapId;

/**
 * @author Mark Paluch
 */
@Table
@Data
public class TypeWithMapId implements MapIdentifiable {

	@PrimaryKeyColumn(type = PrimaryKeyType.PARTITIONED, ordinal = 1) private String firstname;
	@PrimaryKeyColumn(type = PrimaryKeyType.PARTITIONED, ordinal = 2) private String lastname;

	@Override
	public MapId getMapId() {
		return BasicMapId.id("firstname", firstname).with("lastname", lastname);
	}
}
