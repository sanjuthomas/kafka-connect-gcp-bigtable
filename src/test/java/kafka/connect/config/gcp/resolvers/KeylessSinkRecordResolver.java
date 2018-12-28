package kafka.connect.config.gcp.resolvers;

import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;

/**
 *
 * @author Sanju Thomas
 *
 */
public class KeylessSinkRecordResolver extends SinkRecordResolver {

  @Override
  public Object resolveParameter(final ParameterContext parameterContext,
      final ExtensionContext extensionContext) throws ParameterResolutionException {
    return new SinkRecord("demo-topic", 0, null, null, null, super.createData(), -1);
  }
}
