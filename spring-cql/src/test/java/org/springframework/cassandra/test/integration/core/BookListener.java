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
package org.springframework.cassandra.test.integration.core;

import org.springframework.cassandra.core.AsynchronousQueryListener;
import org.springframework.cassandra.test.integration.support.CallbackSynchronizationSupport;

import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;

/**
 * Test Implementation of the {@link AsynchronousQueryListener}
 *
 * @author David Webb
 * @author Matthew T. Adams
 * @author Mark Paluch
 */
public class BookListener extends CallbackSynchronizationSupport implements AsynchronousQueryListener {

	private Book book;
	private boolean done;

	@Override
	public void onQueryComplete(ResultSetFuture resultSetFuture) {

		Row row;

		try {
			row = resultSetFuture.get().one();
		} catch (Exception e) {
			throw new RuntimeException("Failed to get ResultSet from ResultSetFuture", e);
		}

		book = new Book();
		book.setIsbn(row.getString("isbn"));
		book.setTitle(row.getString("title"));
		book.setAuthor(row.getString("author"));
		book.setPages(row.getInt("pages"));

		done = true;

		countDown();
	}

	/**
	 * @return Returns the done.
	 */
	public boolean isDone() {
		return done;
	}

	/**
	 * @return Returns the book.
	 */
	public Book getBook() {
		return book;
	}
}
