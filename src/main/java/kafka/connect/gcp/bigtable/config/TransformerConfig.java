package kafka.connect.gcp.bigtable.config;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import com.google.common.base.MoreObjects;

/**
 *
 * @author Sanju Thomas
 *
 */
public class TransformerConfig {

  private final List<String> keyQualifiers;
  private final String keyDelimiter;
  private final List<String> families;
  private final Map<String, List<String>> familyToQualifierMapping;

  public TransformerConfig(final List<String> keyQualifiers, final String keyDelimiter,
      final List<String> families, final Map<String, List<String>> familyToQualifierMapping) {
    this.keyQualifiers = keyQualifiers;
    this.keyDelimiter = keyDelimiter;
    this.families = families;
    this.familyToQualifierMapping = familyToQualifierMapping;
  }

  public List<String> familyQualifiers(final String family) {
    List<String> list = null;
    if (this.familyToQualifierMapping != null) {
      list = this.familyToQualifierMapping.get(family);
    }
    return list == null ? Collections.emptyList() : list;
  }

  public List<String> families() {
    return this.families;
  }

  public List<String> keyQualifies() {
    return MoreObjects.firstNonNull(this.keyQualifiers, new ArrayList<String>(0));
  }

  public String keyDelimiter() {
    return MoreObjects.firstNonNull(this.keyDelimiter, "_");
  }
}
