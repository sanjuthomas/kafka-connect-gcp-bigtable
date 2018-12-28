package kafka.connect.config.gcp.bigtable;

import java.io.IOException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;

public class ConfigMangerTest {

  private ConfigManger configManger;

  @BeforeAll
  public static void setup() throws JsonParseException, JsonMappingException, IOException {
    ConfigManger.load("src/main/resources/configs/", "demo-topic");
  }

  @Test
  public void test() {

  }

}
