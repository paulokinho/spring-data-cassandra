/*
 *  Copyright 2013-2017 the original author or authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.springframework.data.cassandra.mapping;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.springframework.data.cassandra.mapping.CassandraPersistentPropertyComparator.*;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.cassandra.core.cql.CqlIdentifier;

/**
 * The CassandraPersistentPropertyComparatorUnitTests class is a test suite of test cases testing the contract and
 * functionality of the {@link CassandraPersistentPropertyComparator} class.
 *
 * @author John Blum
 * @since 1.5.0
 */
@RunWith(MockitoJUnitRunner.class)
public class CassandraPersistentPropertyComparatorUnitTests {

	@Mock CassandraPersistentProperty left;

	@Mock CassandraPersistentProperty right;

	@Test // DATACASS-248
	public void leftAndRightAreNullReturnsZero() {

		assertThat(INSTANCE.compare(null, null)).isEqualTo(0);

		verifyZeroInteractions(left);
		verifyZeroInteractions(right);
	}

	@Test // DATACASS-248
	public void leftIsNotNullAndRightIsNullReturnsOne() {

		assertThat(INSTANCE.compare(left, null)).isEqualTo(1);

		verifyZeroInteractions(left);
		verifyZeroInteractions(right);
	}

	@Test // DATACASS-248
	public void leftIsNullAndRightIsNotNullReturnsMinusOne() {

		assertThat(INSTANCE.compare(null, right)).isEqualTo(-1);

		verifyZeroInteractions(left);
		verifyZeroInteractions(right);
	}

	@Test // DATACASS-248
	public void leftAndRightAreEqualReturnsZero() {

		assertThat(INSTANCE.compare(left, left)).isEqualTo(0);
		assertThat(INSTANCE.compare(right, right)).isEqualTo(0);

		verifyZeroInteractions(left);
		verifyZeroInteractions(right);
	}

	@Test // DATACASS-248
	public void leftAndRightAreCompositePrimaryKeysReturnsZero() {

		when(left.isCompositePrimaryKey()).thenReturn(true);
		when(right.isCompositePrimaryKey()).thenReturn(true);

		assertThat(INSTANCE.compare(left, right)).isEqualTo(0);

		verify(left, times(1)).isCompositePrimaryKey();
		verify(right, times(1)).isCompositePrimaryKey();
	}

	@Test // DATACASS-248
	public void leftIsCompositePrimaryKeyReturnsMinusOne() {

		when(left.isCompositePrimaryKey()).thenReturn(true);
		when(left.isPrimaryKeyColumn()).thenReturn(false);
		when(right.isCompositePrimaryKey()).thenReturn(false);
		when(right.isPrimaryKeyColumn()).thenReturn(false);

		assertThat(INSTANCE.compare(left, right)).isEqualTo(-1);

		verify(left, times(1)).isCompositePrimaryKey();
		verify(left, times(1)).isPrimaryKeyColumn();
		verify(right, times(1)).isCompositePrimaryKey();
		verify(right, times(1)).isPrimaryKeyColumn();
	}

	@Test // DATACASS-248
	public void leftIsPrimaryKeyColumnReturnsMinusOne() {

		when(left.isCompositePrimaryKey()).thenReturn(false);
		when(left.isPrimaryKeyColumn()).thenReturn(true);
		when(right.isCompositePrimaryKey()).thenReturn(false);
		when(right.isPrimaryKeyColumn()).thenReturn(false);

		assertThat(INSTANCE.compare(left, right)).isEqualTo(-1);

		verify(left, times(1)).isCompositePrimaryKey();
		verify(left, times(1)).isPrimaryKeyColumn();
		verify(right, times(1)).isCompositePrimaryKey();
		verify(right, times(1)).isPrimaryKeyColumn();
	}

	@Test // DATACASS-248
	public void rightIsCompositePrimaryKeyReturnsOne() {

		when(left.isCompositePrimaryKey()).thenReturn(false);
		when(left.isPrimaryKeyColumn()).thenReturn(false);
		when(right.isCompositePrimaryKey()).thenReturn(true);
		when(right.isPrimaryKeyColumn()).thenReturn(false);

		assertThat(INSTANCE.compare(left, right)).isEqualTo(1);

		verify(left, times(1)).isCompositePrimaryKey();
		verify(left, times(1)).isPrimaryKeyColumn();
		verify(right, times(1)).isCompositePrimaryKey();
		verify(right, times(1)).isPrimaryKeyColumn();
	}

	@Test // DATACASS-248
	public void rightIsPrimaryKeyColumnReturnsOne() {

		when(left.isCompositePrimaryKey()).thenReturn(false);
		when(left.isPrimaryKeyColumn()).thenReturn(false);
		when(right.isCompositePrimaryKey()).thenReturn(false);
		when(right.isPrimaryKeyColumn()).thenReturn(true);

		assertThat(INSTANCE.compare(left, right)).isEqualTo(1);

		verify(left, times(1)).isCompositePrimaryKey();
		verify(left, times(1)).isPrimaryKeyColumn();
		verify(right, times(1)).isCompositePrimaryKey();
		verify(right, times(1)).isPrimaryKeyColumn();
	}

	@Test // DATACASS-248
	public void compareLeftAndRightNamesReturnsNegativeValue() {

		when(left.isCompositePrimaryKey()).thenReturn(false);
		when(left.isPrimaryKeyColumn()).thenReturn(true);
		when(right.isCompositePrimaryKey()).thenReturn(true);
		when(right.isPrimaryKeyColumn()).thenReturn(false);
		when(left.getColumnName()).thenReturn(CqlIdentifier.cqlId("left"));
		when(right.getColumnName()).thenReturn(CqlIdentifier.cqlId("right"));

		assertThat(INSTANCE.compare(left, right)).isLessThan(0);

		verify(left, times(1)).isCompositePrimaryKey();
		verify(left, times(1)).isPrimaryKeyColumn();
		verify(left, times(1)).getColumnName();
		verify(right, times(1)).isCompositePrimaryKey();
		verify(right, times(1)).isPrimaryKeyColumn();
		verify(right, times(1)).getColumnName();
	}

	@Test // DATACASS-352
	public void columnNameComparisonShouldHonorContract() throws Exception {

		BasicCassandraMappingContext context = new BasicCassandraMappingContext();
		CassandraPersistentEntity<?> persistentEntity = context.getPersistentEntity(TwoColumns.class);

		CassandraPersistentProperty annotated = persistentEntity.getPersistentProperty("annotated");
		CassandraPersistentProperty another = persistentEntity.getPersistentProperty("anotherAnnotated");
		CassandraPersistentProperty plain = persistentEntity.getPersistentProperty("plain");

		assertThat(INSTANCE.compare(annotated, plain)).isLessThanOrEqualTo(-1);
		assertThat(INSTANCE.compare(plain, annotated)).isGreaterThanOrEqualTo(1);
		assertThat(INSTANCE.compare(another, another)).isEqualTo(0);
	}

	static class TwoColumns {

		@Column("annotated") String annotated;

		@Column("another") String anotherAnnotated;

		String plain;
	}
}
