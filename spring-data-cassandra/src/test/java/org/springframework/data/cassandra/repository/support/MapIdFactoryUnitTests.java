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

package org.springframework.data.cassandra.repository.support;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.cassandra.repository.support.IdInterfaceValidator.*;
import static org.springframework.data.cassandra.repository.support.MapIdFactory.*;

import java.io.Serializable;
import java.util.Random;

import org.junit.Test;
import org.springframework.data.cassandra.repository.MapId;

/**
 * Unit tests for {@link MapIdFactory}.
 *
 * @author Matthew T. Adams
 */
public class MapIdFactoryUnitTests {

	interface HappyExtendingMapIdAndSerializable extends MapId, Serializable {
		HappyExtendingMapIdAndSerializable string(String s);

		void setString(String s);

		HappyExtendingMapIdAndSerializable withString(String s);

		String string();

		String getString();

		HappyExtendingMapIdAndSerializable number(Integer i);

		void setNumber(Integer i);

		Integer number();

		Integer getNumber();
	}

	@Test
	public void testHappyExtendingMapId() {
		Random r = new Random();
		String s = "" + r.nextInt();
		Integer i = new Integer(r.nextInt());

		HappyExtendingMapIdAndSerializable id = id(HappyExtendingMapIdAndSerializable.class);

		assertThat(id.string()).isNull();
		assertThat(id.number()).isNull();
		assertThat(id.getString()).isNull();
		assertThat(id.getNumber()).isNull();

		id.setNumber(i);
		assertThat(id.getNumber()).isEqualTo(i);
		assertThat(id.number()).isEqualTo(i);
		assertThat(id.get("number")).isEqualTo(i);

		HappyExtendingMapIdAndSerializable returned = null;

		returned = id.number(i = r.nextInt());
		assertThat(id).isSameAs(returned);
		assertThat(id.getNumber()).isEqualTo(i);
		assertThat(id.number()).isEqualTo(i);
		assertThat(id.get("number")).isEqualTo(i);

		id.put("number", i = r.nextInt());
		assertThat(id.getNumber()).isEqualTo(i);
		assertThat(id.number()).isEqualTo(i);
		assertThat(id.get("number")).isEqualTo(i);

		id.setString(s);
		assertThat(id.getString()).isEqualTo(s);
		assertThat(id.string()).isEqualTo(s);
		assertThat(id.get("string")).isEqualTo(s);

		returned = id.string(s = "" + r.nextInt());
		assertThat(id).isSameAs(returned);
		assertThat(id.getString()).isEqualTo(s);
		assertThat(id.string()).isEqualTo(s);
		assertThat(id.get("string")).isEqualTo(s);

		returned = id.withString(s = "" + r.nextInt());
		assertThat(id).isSameAs(returned);
		assertThat(id.getString()).isEqualTo(s);
		assertThat(id.string()).isEqualTo(s);
		assertThat(id.get("string")).isEqualTo(s);

		id.put("string", s = "" + r.nextInt());
		assertThat(id.getString()).isEqualTo(s);
		assertThat(id.string()).isEqualTo(s);
		assertThat(id.get("string")).isEqualTo(s);

		id.setString(null);
		assertThat(id.getString()).isNull();
		assertThat(id.string()).isNull();
		assertThat(id.get("string")).isNull();

		id.setNumber(null);
		assertThat(id.getNumber()).isNull();
		assertThat(id.number()).isNull();
		assertThat(id.get("number")).isNull();
	}

	interface HappyExtendingNothing {
		HappyExtendingNothing string(String s);

		void setString(String s);

		HappyExtendingNothing withString(String s);

		String string();

		String getString();

		HappyExtendingNothing number(Integer i);

		void setNumber(Integer i);

		Integer number();

		Integer getNumber();
	}

	@Test
	public void testHappyExtendingNothing() {
		Random r = new Random();
		String s = "" + r.nextInt();
		Integer i = new Integer(r.nextInt());

		HappyExtendingNothing id = id(HappyExtendingNothing.class);

		assertThat(id instanceof Serializable).isTrue();
		assertThat(id instanceof MapId).isTrue();
		MapId mapid = (MapId) id;

		assertThat(id.string()).isNull();
		assertThat(id.number()).isNull();
		assertThat(id.getString()).isNull();
		assertThat(id.getNumber()).isNull();

		id.setNumber(i);
		assertThat(id.getNumber()).isEqualTo(i);
		assertThat(id.number()).isEqualTo(i);
		assertThat(mapid.get("number")).isEqualTo(i);

		HappyExtendingNothing returned = null;

		returned = id.number(i = r.nextInt());
		assertThat(id).isSameAs(returned);
		assertThat(id.getNumber()).isEqualTo(i);
		assertThat(id.number()).isEqualTo(i);
		assertThat(mapid.get("number")).isEqualTo(i);

		mapid.put("number", i = r.nextInt());
		assertThat(id.getNumber()).isEqualTo(i);
		assertThat(id.number()).isEqualTo(i);
		assertThat(mapid.get("number")).isEqualTo(i);

		id.setString(s);
		assertThat(id.getString()).isEqualTo(s);
		assertThat(id.string()).isEqualTo(s);
		assertThat(mapid.get("string")).isEqualTo(s);

		returned = id.string(s = "" + r.nextInt());
		assertThat(id).isSameAs(returned);
		assertThat(id.getString()).isEqualTo(s);
		assertThat(id.string()).isEqualTo(s);
		assertThat(mapid.get("string")).isEqualTo(s);

		returned = id.withString(s = "" + r.nextInt());
		assertThat(id).isSameAs(returned);
		assertThat(id.getString()).isEqualTo(s);
		assertThat(id.string()).isEqualTo(s);
		assertThat(mapid.get("string")).isEqualTo(s);

		mapid.put("string", s = "" + r.nextInt());
		assertThat(id.getString()).isEqualTo(s);
		assertThat(id.string()).isEqualTo(s);
		assertThat(mapid.get("string")).isEqualTo(s);

		id.setString(null);
		assertThat(id.getString()).isNull();
		assertThat(id.string()).isNull();
		assertThat(mapid.get("string")).isNull();

		id.setNumber(null);
		assertThat(id.getNumber()).isNull();
		assertThat(id.number()).isNull();
		assertThat(mapid.get("number")).isNull();
	}

	class IdClass {}

	interface Foo {}

	interface IdExtendingNotMapId extends Foo {}

	interface LiteralGet {
		String get();
	}

	interface GetterReturningVoid {
		void getString();
	}

	interface GetReturningVoid {
		void string();
	}

	interface GetReturningNonSerializable {
		Object getFoo();
	}

	interface MethodWithMoreThanOneArgument {
		void foo(Object a, Object b);
	}

	interface LiteralSet {
		void set(String s);
	}

	interface LiteralWith {
		void with(String s);
	}

	interface SetterMethodNotReturningVoidOrThis {
		String string(String s);
	}

	interface SetMethodNotReturningVoidOrThis {
		String setString(String s);
	}

	interface WithMethodNotReturningVoidOrThis {
		String withString(String s);
	}

	interface SetterMethodTakingNonSerializable {
		void string(Object o);
	}

	interface SetMethodTakingNonSerializable {
		void string(Object o);
	}

	interface WithMethodTakingNonSerializable {
		void string(Object o);
	}

	@Test
	public void testUnhappies() {
		Class<?>[] interfaces = new Class<?>[] { IdClass.class, IdExtendingNotMapId.class, LiteralGet.class,
				GetterReturningVoid.class, GetReturningVoid.class, GetReturningNonSerializable.class,
				MethodWithMoreThanOneArgument.class, LiteralSet.class, LiteralWith.class,
				SetterMethodNotReturningVoidOrThis.class, SetMethodNotReturningVoidOrThis.class,
				WithMethodNotReturningVoidOrThis.class, SetterMethodTakingNonSerializable.class,
				SetMethodTakingNonSerializable.class, WithMethodTakingNonSerializable.class };
		for (Class<?> i : interfaces) {
			try {
				validate(i);
				fail("should've caught IdInterfaceException validating interface " + i);
			} catch (IdInterfaceExceptions e) {
				assertThat(e.getCount()).isEqualTo(1);
			}
		}
	}
}
