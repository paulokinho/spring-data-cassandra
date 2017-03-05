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
package org.springframework.cassandra.core.cql;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.cassandra.core.cql.CqlIdentifier.*;

import org.junit.Test;
import org.springframework.cassandra.core.ReservedKeyword;

/**
 * Unit tests for {@link CqlIdentifier}.
 *
 * @author John McPeek
 * @author Matthew T. Adams
 */
public class CqlIdentifierUnitTests {

	@Test
	public void testUnquotedIdentifiers() {

		String[] ids = new String[] { "foo", "Foo", "FOO", "a_", "a1" };

		for (String id : ids) {
			CqlIdentifier cqlId = cqlId(id);
			assertThat(cqlId.isQuoted()).isFalse();
			assertThat(cqlId.toCql()).isEqualTo(id.toLowerCase());
		}
	}

	@Test
	public void testForceQuotedIdentifiers() {

		String[] ids = new String[] { "foo", "Foo", "FOO", "a_", "a1" };

		for (String id : ids) {
			CqlIdentifier cqlId = quotedCqlId(id);
			assertThat(cqlId.isQuoted()).isTrue();
			assertThat(cqlId.toCql()).isEqualTo("\"" + id + "\"");
		}
	}

	@Test
	public void testReservedWordsEndUpQuoted() {

		for (ReservedKeyword id : ReservedKeyword.values()) {
			CqlIdentifier cqlId = cqlId(id.name());
			assertThat(cqlId.isQuoted()).isTrue();
			assertThat(cqlId.toCql()).isEqualTo("\"" + id.name() + "\"");

			cqlId = cqlId(id.name().toLowerCase());
			assertThat(cqlId.isQuoted()).isTrue();
			assertThat(cqlId.toCql()).isEqualTo("\"" + id.name().toLowerCase() + "\"");
		}
	}

	@Test
	public void testIllegals() {
		String[] illegals = new String[] { null, "", "a ", "a a", "a\"", "a'", "a''", "\"\"", "''", "-", "a-", "_", "_a" };
		for (String illegal : illegals) {
			try {
				cqlId(illegal);
				fail(String.format("identifier [%s] should have caused IllegalArgumentException", illegal));
			} catch (IllegalArgumentException x) {
				// :)
			}
		}
	}
}
