package com.redis.trino;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.lettuce.core.search.arguments.CreateArgs;
import io.lettuce.core.search.arguments.GeoFieldArgs;
import io.lettuce.core.search.arguments.NumericFieldArgs;
import io.lettuce.core.search.arguments.TagFieldArgs;
import io.lettuce.core.search.arguments.TextFieldArgs;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;

public class TestNativeSearch extends AbstractTestQueryFramework {
    protected RedisEnterprise.Deployment deployment() { return RedisEnterprise.Deployment.NON_SHARDED; }

    @Override
    protected QueryRunner createQueryRunner() throws Exception {
        RediSearchServer server = closeAfterClass(new RediSearchServer(deployment()));
        var redis = server.getConnection().sync();
        redis.ftCreate("native_places", CreateArgs.builder().withPrefix("native_place:").build(),
                List.of(TagFieldArgs.builder().name("id").build(), TextFieldArgs.builder().name("name").build(),
                        NumericFieldArgs.builder().name("rating").build(), GeoFieldArgs.builder().name("location").build()));
        server.awaitIndexed("native_places");
        redis.hset("native_place:1", Map.of("id", "1", "name", "Redis database", "rating", "5", "location", "-122.4194,37.7749"));
        redis.hset("native_place:2", Map.of("id", "2", "name", "Redis search", "rating", "3", "location", "-122.42,37.775"));
        redis.hset("native_place:3", Map.of("id", "3", "name", "Another engine", "rating", "4", "location", "-74.006,40.7128"));
        redis.ftCreate("native_json_places", CreateArgs.builder().on(CreateArgs.TargetType.JSON).withPrefix("native_json:").build(),
                List.of(TagFieldArgs.builder().name("$.id").as("id").build(), TextFieldArgs.builder().name("$.name").as("name").build()));
        server.awaitIndexed("native_json_places");
        redis.jsonSet("native_json:1", io.lettuce.core.json.JsonPath.ROOT_PATH, "{\"id\":\"j1\",\"name\":\"Redis JSON\"}");
        redis.jsonSet("native_json:2", io.lettuce.core.json.JsonPath.ROOT_PATH, "{\"id\":\"j2\",\"name\":\"Other JSON\"}");
        return RediSearchQueryRunner.createRediSearchQueryRunner(server, List.of(), Map.of(), Map.of("redisearch.cursor-count", "1"));
    }

    @Test
    public void testTextSearchComposesWithSql() {
        assertThat(query("SELECT id FROM TABLE(redisearch.system.search('native_places', '@name:redis'))"))
                .isFullyPushedDown().matches("VALUES VARCHAR '1', '2'");
        assertThat(query("SELECT id FROM TABLE(redisearch.system.search('native_places', '@name:redis')) WHERE rating > 4"))
                .isFullyPushedDown().matches("VALUES VARCHAR '1'");
        assertThat(query("SELECT count(*) FROM TABLE(redisearch.system.search(TABLE_NAME => 'native_places', "
                + "QUERY => '@name:redis', SCHEMA_NAME => 'tpch'))"))
                .isFullyPushedDown().matches("VALUES BIGINT '2'");
        assertThat(query("SELECT __key FROM TABLE(redisearch.system.search('native_places', '@name:search'))"))
                .matches("VALUES VARCHAR 'native_place:2'");
        // A native OR must stay grouped when a SQL predicate is added.
        assertThat(query("SELECT id FROM TABLE(redisearch.system.search('native_places', '@name:redis | @name:engine')) WHERE rating > 3"))
                .matches("VALUES VARCHAR '1', '3'");
    }

    @Test
    public void testGeoSearch() {
        assertThat(query("SELECT id FROM TABLE(redisearch.system.search('native_places', '@location:[-122.4194 37.7749 1 km]'))"))
                .isFullyPushedDown().matches("VALUES VARCHAR '1', '2'");
        assertThat(query("SELECT count(*) FROM TABLE(redisearch.system.search('native_places', '@location:[-74.006 40.7128 1 km]'))"))
                .isFullyPushedDown().matches("VALUES BIGINT '1'");
    }

    @Test
    public void testJsonNativePredicateUsesTypedCursorReader() {
        assertThat(query("SELECT id FROM TABLE(redisearch.system.search('native_json_places', '@name:redis'))"))
                .isFullyPushedDown().matches("VALUES VARCHAR 'j1'");
        assertThat(query("SELECT count(*) FROM TABLE(redisearch.system.search('native_json_places', '@name:redis'))"))
                .isFullyPushedDown().matches("VALUES BIGINT '1'");
    }

    @Test
    public void testInvalidArgumentsAndNativeSyntax() {
        assertQueryFails("SELECT * FROM TABLE(redisearch.system.search('native_places', ''))", ".*QUERY must be a non-empty string.*");
        assertQueryFails("SELECT * FROM TABLE(redisearch.system.search('', '*'))", ".*TABLE_NAME must be a non-empty string.*");
        assertQueryFails("SELECT * FROM TABLE(redisearch.system.search('missing_places', '*'))", "Index 'missing_places' not found");
        assertQueryFails("SELECT * FROM TABLE(redisearch.system.search('native_places', '*', 'missing_schema'))", ".*missing_schema.*(not found|does not exist).*" );
        assertQueryFails("SELECT * FROM TABLE(redisearch.system.search('native_places', '@name:('))", ".*(Syntax|syntax).*" );
    }
}
