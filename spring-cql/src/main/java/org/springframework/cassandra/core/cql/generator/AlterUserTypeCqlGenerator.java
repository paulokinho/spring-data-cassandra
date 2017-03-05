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

import static org.springframework.cassandra.core.cql.CqlStringUtils.noNull;

import org.springframework.cassandra.core.keyspace.AddColumnSpecification;
import org.springframework.cassandra.core.keyspace.AlterColumnSpecification;
import org.springframework.cassandra.core.keyspace.AlterUserTypeSpecification;
import org.springframework.cassandra.core.keyspace.ColumnChangeSpecification;
import org.springframework.cassandra.core.keyspace.RenameColumnSpecification;
import org.springframework.util.Assert;

/**
 * CQL generator for generating {@code ALTER TYPE} statements.
 *
 * @author Fabio J. Mendes
 * @author Mark Paluch
 * @since 1.5
 * @see AlterUserTypeSpecification
 * @see AddColumnSpecification
 * @see RenameColumnSpecification
 * @see AlterColumnSpecification
 */
public class AlterUserTypeCqlGenerator extends UserTypeNameCqlGenerator<AlterUserTypeSpecification> {

	public static String toCql(AlterUserTypeSpecification specification) {
		return new AlterUserTypeCqlGenerator(specification).toCql();
	}

	/**
	 * Creates a new {@link AlterUserTypeCqlGenerator} for a {@link AlterUserTypeSpecification}.
	 *
	 * @param specification must not be {@literal null}.
	 */
	public AlterUserTypeCqlGenerator(AlterUserTypeSpecification specification) {
		super(specification);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.core.cql.generator.UserTypeNameCqlGenerator#toCql(java.lang.StringBuilder)
	 */
	@Override
	public StringBuilder toCql(StringBuilder cql) {

		Assert.notNull(getSpecification().getName(), "User type name must not be null");

		Assert.isTrue(!getSpecification().getChanges().isEmpty(),
				String.format("User type [%s] does not contain fields", getSpecification().getName()));

		return changesCql(preambleCql(cql)).append(";");
	}

	private StringBuilder preambleCql(StringBuilder cql) {
		return noNull(cql).append("ALTER TYPE ").append(spec().getName()).append(' ');
	}

	private StringBuilder changesCql(StringBuilder cql) {
		cql = noNull(cql);

		boolean first = true;
		boolean lastChangeWasRename = false;

		for (ColumnChangeSpecification change : spec().getChanges()) {
			if (!first) {
				cql.append(' ');
			}

			getCqlGeneratorFor(change, lastChangeWasRename).toCql(cql);
			lastChangeWasRename = change instanceof RenameColumnSpecification;
			first = false;
		}

		return cql;
	}

	private ColumnChangeCqlGenerator<?> getCqlGeneratorFor(ColumnChangeSpecification change,
			boolean lastChangeWasRename) {

		if (change instanceof AddColumnSpecification) {
			return new AddColumnCqlGenerator((AddColumnSpecification) change);
		}

		if (change instanceof AlterColumnSpecification) {
			return new AlterColumnCqlGenerator((AlterColumnSpecification) change);
		}

		if (change instanceof RenameColumnSpecification) {
			return new RenameColumnCqlGenerator(lastChangeWasRename ? "AND" : RenameColumnCqlGenerator.RENAME, change);
		}

		throw new IllegalArgumentException(
				String.format("Unknown ColumnChangeSpecification type: %s", change.getClass().getName()));
	}
}
