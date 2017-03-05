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
package org.springframework.cassandra.core;

import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * CQL keywords.
 *
 * @see <a href=
 *      "http://cassandra.apache.org/doc/cql3/CQL.html#appendixA">http://cassandra.apache.org/doc/cql3/CQL.html#appendixA</a>
 * @author Matthew T. Adams
 */
public enum ReservedKeyword {
	ADD,
	ALTER,
	AND,
	ANY,
	APPLY,
	ASC,
	AUTHORIZE,
	BATCH,
	BEGIN,
	BY,
	COLUMNFAMILY,
	CREATE,
	DELETE,
	DESC,
	DROP,
	EACH_QUORUM,
	FROM,
	GRANT,
	IN,
	INDEX,
	INSERT,
	INTO,
	KEYSPACE,
	LIMIT,
	LOCAL_ONE,
	LOCAL_QUORUM,
	MODIFY,
	NORECURSIVE,
	OF,
	ON,
	ONE,
	ORDER,
	PRIMARY,
	QUORUM,
	REVOKE,
	SCHEMA,
	SELECT,
	SET,
	TABLE,
	THREE,
	TOKEN,
	TRUNCATE,
	TWO,
	UPDATE,
	USE,
	USING,
	WHERE,
	WITH;

	/**
	 * @see ReservedKeyword#isReserved(String)
	 */
	public static boolean isReserved(CharSequence candidate) {

		Assert.notNull(candidate, "CharSequence must not be null");

		return isReserved(candidate.toString());
	}

	/**
	 * Returns whether the given string is a CQL reserved keyword. This comparison is done regardless of case.
	 */
	public static boolean isReserved(String candidate) {

		if (!StringUtils.hasText(candidate)) {
			return false;
		}

		try {
			Enum.valueOf(ReservedKeyword.class, candidate.toUpperCase());
			return true;
		} catch (IllegalArgumentException x) {
			return false;
		}
	}
}
