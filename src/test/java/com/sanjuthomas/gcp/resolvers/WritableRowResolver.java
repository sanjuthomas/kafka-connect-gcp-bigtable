package com.sanjuthomas.gcp.resolvers;

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import com.sanjuthomas.gcp.bigtable.bean.WritableRow;

/**
 * 
 * @author Sanju Thomas
 *
 */
public class WritableRowResolver implements ParameterResolver {

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
