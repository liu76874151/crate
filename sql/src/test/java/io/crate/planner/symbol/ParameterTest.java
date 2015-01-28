/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.planner.symbol;

import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.HashMap;

import static io.crate.testing.TestingHelpers.assertLiteralSymbol;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

public class ParameterTest {

    @Test
    public void testResolveBoolean() throws Exception {
        assertThat(Literal.fromParameter(new Parameter(true)), Matchers.<Literal>is(Literal.newLiteral(true)));
        assertThat(Literal.fromParameter(new Parameter(false)), Matchers.<Literal>is(Literal.newLiteral(false)));

        assertEquals(Literal.NULL, Literal.fromParameter(new Parameter(null)));
    }

    @Test
    public void testResolveObject() throws Exception {
        Literal<?> literal = Literal.fromParameter(new Parameter(new HashMap<String, Object>()));
        assertLiteralSymbol(literal, new HashMap<String, Object>());
    }

    @Test
    public void testFromLongParameter() throws Exception {
        assertEquals(Literal.newLiteral(1L), Literal.fromParameter(new Parameter(1L)));
    }


    @Test
    public void testParameterFromString() throws Exception {
        assertEquals(Literal.newLiteral("123"), Literal.fromParameter(new Parameter("123")));
    }
}