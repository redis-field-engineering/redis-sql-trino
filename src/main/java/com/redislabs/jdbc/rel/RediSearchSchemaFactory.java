package com.redislabs.jdbc.rel;

import com.redislabs.lettusearch.RediSearchClient;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.util.Arrays;
import java.util.Map;


/**
 * Factory that creates a {@link RediSearchSchema}.
 */
@SuppressWarnings("UnusedDeclaration")
public class RediSearchSchemaFactory implements SchemaFactory {

  public static final String REDIS_URI = "uri";
  public static final String INDEXES = "indexes";
  public static final String COMMA_DELIMITER = ",";

  public RediSearchSchemaFactory() {
    // Do Nothing
  }

  @Override public synchronized Schema create(SchemaPlus parentSchema, String name,
      Map<String, Object> operand) {
    Map map = (Map) operand;
    String uri = (String) map.get(REDIS_URI);
    String[] indexNames = ((String) map.get(INDEXES)).split(COMMA_DELIMITER);
    RediSearchClient rediSearchClient = RediSearchClient.create(uri);
    return new RediSearchSchema(rediSearchClient.connect(), Arrays.asList(indexNames));
  }
}
