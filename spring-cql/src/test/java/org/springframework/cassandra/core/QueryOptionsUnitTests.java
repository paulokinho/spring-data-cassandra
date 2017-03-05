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
package org.springframework.cassandra.core;

import static org.assertj.core.api.Assertions.*;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.FallthroughRetryPolicy;
import com.datastax.driver.core.policies.LoggingRetryPolicy;

/**
 * Unit tests for {@link QueryOptions}.
 *
 * @author Mark Paluch
 */
public class QueryOptionsUnitTests {

	@Test // DATACASS-202
	public void buildQueryOptions() {

		QueryOptions queryOptions = QueryOptions.builder() //
				.consistencyLevel(ConsistencyLevel.ANY) //
				.retryPolicy(RetryPolicy.DEFAULT) //
				.readTimeout(1, TimeUnit.SECONDS)//
				.fetchSize(10)//
				.tracing(true)//
				.build(); //

		assertThat(queryOptions.getClass()).isEqualTo(QueryOptions.class);
		assertThat(queryOptions.getRetryPolicy()).isEqualTo(RetryPolicy.DEFAULT);
		assertThat(queryOptions.getConsistencyLevel()).isNull();
		assertThat(queryOptions.getDriverConsistencyLevel()).isEqualTo(ConsistencyLevel.ANY);
		assertThat(queryOptions.getReadTimeout()).isEqualTo(1000);
		assertThat(queryOptions.getFetchSize()).isEqualTo(10);
		assertThat(queryOptions.getTracing()).isTrue();
	}

	@Test // DATACASS-202
	public void buildQueryOptionsWithDriverRetryPolicy() {

		QueryOptions writeOptions = QueryOptions.builder() //
				.retryPolicy(new LoggingRetryPolicy(DefaultRetryPolicy.INSTANCE)) //
				.build(); //

		assertThat(writeOptions.getRetryPolicy()).isNull();
		assertThat(writeOptions.getDriverRetryPolicy()).isInstanceOf(LoggingRetryPolicy.class);
	}

	@Test // DATACASS-202
	public void buildQueryOptionsWithRetryPolicy() {

		QueryOptions writeOptions = QueryOptions.builder() //
				.retryPolicy(RetryPolicy.DOWNGRADING_CONSISTENCY) //
				.build(); //

		assertThat(writeOptions.getRetryPolicy()).isEqualTo(RetryPolicy.DOWNGRADING_CONSISTENCY);
		assertThat(writeOptions.getDriverRetryPolicy()).isNull();
	}

	@Test(expected = IllegalStateException.class) // DATACASS-202
	public void builderShouldRejectSettingOurAndDriverRetryPolicy() {
		QueryOptions.builder().retryPolicy(RetryPolicy.DEFAULT).retryPolicy(FallthroughRetryPolicy.INSTANCE);
	}

	@Test(expected = IllegalStateException.class) // DATACASS-202
	public void builderShouldRejectSettingDriverAndOurRetryPolicy() {
		QueryOptions.builder().retryPolicy(FallthroughRetryPolicy.INSTANCE).retryPolicy(RetryPolicy.DEFAULT);
	}

	@Test(expected = IllegalStateException.class) // DATACASS-202
	public void shouldRejectSettingOurAndDriverRetryPolicy() {

		QueryOptions queryOptions = new QueryOptions();
		queryOptions.setRetryPolicy(RetryPolicy.DEFAULT);
		queryOptions.setRetryPolicy(FallthroughRetryPolicy.INSTANCE);
	}

	@Test(expected = IllegalStateException.class) // DATACASS-202
	public void shouldRejectSettingDriverAndOurRetryPolicy() {

		QueryOptions queryOptions = new QueryOptions();
		queryOptions.setRetryPolicy(FallthroughRetryPolicy.INSTANCE);
		queryOptions.setRetryPolicy(RetryPolicy.DEFAULT);
	}

	@Test(expected = IllegalStateException.class) // DATACASS-202
	public void shouldRejectSettingOurAndDriverConsistencyLevel() {

		QueryOptions queryOptions = new QueryOptions();
		queryOptions.setConsistencyLevel(org.springframework.cassandra.core.ConsistencyLevel.ANY);
		queryOptions.setConsistencyLevel(ConsistencyLevel.ANY);
	}

	@Test(expected = IllegalStateException.class) // DATACASS-202
	public void shouldRejectSettingDriverAndOurConsistencyLevel() {

		QueryOptions queryOptions = new QueryOptions();
		queryOptions.setConsistencyLevel(ConsistencyLevel.ANY);
		queryOptions.setConsistencyLevel(org.springframework.cassandra.core.ConsistencyLevel.ANY);
	}
}
