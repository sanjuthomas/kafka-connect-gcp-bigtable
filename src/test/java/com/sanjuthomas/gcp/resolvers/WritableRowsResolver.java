/*
 *
 *  Copyright (c) 2023 Sanju Thomas
 *
 *  Licensed under the MIT License (the "License");
 *  you may not use this file except in compliance with the License.
 *
 *  You may obtain a copy of the License at https://en.wikipedia.org/wiki/MIT_License
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 *  either express or implied.  See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 */

package com.sanjuthomas.gcp.resolvers;

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import com.sanjuthomas.gcp.bigtable.bean.WritableRow;

/**
 * @author Sanju Thomas
 */
public class WritableRowsResolver implements ParameterResolver {

  @Override
  public boolean supportsParameter(ParameterContext parameterContext,
    ExtensionContext extensionContext) throws ParameterResolutionException {
    return parameterContext.getParameter().getType() == List.class;
  }

  @Override
  public Object resolveParameter(ParameterContext parameterContext,
    ExtensionContext extensionContext) throws ParameterResolutionException {
    return Arrays.asList(new WritableRow("one"), new WritableRow("two"));
  }

}
