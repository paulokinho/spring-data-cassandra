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

import com.datastax.driver.core.policies.FallthroughRetryPolicy;

/**
 * Unit tests for {@link WriteOptions}.
 *
 * @author Mark Paluch
 */
public class WriteOptionsUnitTests {

	@Test // DATACASS-202
	public void buildWriteOptions() {

		WriteOptions writeOptions = WriteOptions.builder() //
				.consistencyLevel(com.datastax.driver.core.ConsistencyLevel.ANY) //
				.ttl(123) //
				.retryPolicy(RetryPolicy.DEFAULT) //
				.readTimeout(1)//
				.fetchSize(10)//
				.withTracing()//
				.build(); //

		assertThat(writeOptions.getTtl()).isEqualTo(123);
		assertThat(writeOptions.getRetryPolicy()).isEqualTo(RetryPolicy.DEFAULT);
		assertThat(writeOptions.getConsistencyLevel()).isNull();
		assertThat(writeOptions.getDriverConsistencyLevel()).isEqualTo(com.datastax.driver.core.ConsistencyLevel.ANY);
		assertThat(writeOptions.getReadTimeout()).isEqualTo(1);
		assertThat(writeOptions.getFetchSize()).isEqualTo(10);
		assertThat(writeOptions.getTracing()).isTrue();
	}

	@Test // DATACASS-202
	public void buildReadTimeoutOptionsWriteOptions() {

		WriteOptions writeOptions = WriteOptions.builder().readTimeout(1, TimeUnit.MINUTES).build();

		assertThat(writeOptions.getReadTimeout()).isEqualTo(60L * 1000L);
		assertThat(writeOptions.getFetchSize()).isNull();
		assertThat(writeOptions.getTracing()).isNull();
	}

	@Test // DATACASS-202
	public void buildQueryOptionsWithDriverRetryPolicy() {

		QueryOptions writeOptions = QueryOptions.builder().retryPolicy(FallthroughRetryPolicy.INSTANCE).build();

		assertThat(writeOptions.getRetryPolicy()).isNull();
		assertThat(writeOptions.getDriverRetryPolicy()).isEqualTo(FallthroughRetryPolicy.INSTANCE);
	}

	@Test // DATACASS-202
	public void buildQueryOptionsWithRetryPolicy() {

		QueryOptions writeOptions = QueryOptions.builder().retryPolicy(RetryPolicy.DOWNGRADING_CONSISTENCY).build();

		assertThat(writeOptions.getRetryPolicy()).isEqualTo(RetryPolicy.DOWNGRADING_CONSISTENCY);
		assertThat(writeOptions.getDriverRetryPolicy()).isNull();
	}

	@Test(expected = IllegalStateException.class) // DATACASS-202
	public void builderShouldRejectSettingOurAndDriverRetryPolicy() {
		WriteOptions.builder().retryPolicy(RetryPolicy.DEFAULT).retryPolicy(FallthroughRetryPolicy.INSTANCE);
	}

	@Test(expected = IllegalStateException.class) // DATACASS-202
	public void builderShouldRejectSettingDriverAndOurRetryPolicy() {
		WriteOptions.builder().retryPolicy(FallthroughRetryPolicy.INSTANCE).retryPolicy(RetryPolicy.DEFAULT);
	}
}
