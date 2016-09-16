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
package org.springframework.cassandra.core;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.exceptions.DriverException;

import reactor.core.publisher.Mono;

/**
 * Creates a {@link PreparedStatement} for the usage with the DataStax Java Driver.
 * 
 * @author Mark Paluch
 * @since 2.0
 */
public interface ReactivePreparedStatementCreator {

	/**
	 * Create a statement in this session. Allows implementations to use {@link PreparedStatement}s. The
	 * {@link ReactiveCqlTemplate} will attempt to cache the {@link PreparedStatement}s for future use without the
	 * overhead of re-preparing on the entire cluster.
	 * 
	 * @param session Session to use to create statement, must not be {@literal null}.
	 * @return a prepared statement
	 * @throws DriverException there is no need to catch DriverException that may be thrown in the implementation of this
	 *           method. The {@link ReactiveCqlTemplate} class will handle them.
	 */
	Mono<PreparedStatement> createPreparedStatement(ReactiveSession session) throws DriverException;

}
