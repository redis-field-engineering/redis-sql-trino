package com.redislabs.jdbc;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redis.lettucemod.api.search.CreateOptions;
import com.redis.lettucemod.api.search.Field;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.testcontainers.RedisModulesContainer;
import com.redislabs.jdbc.rel.RediSearchSchema;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.test.CalciteAssert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;


@Testcontainers
public abstract class AbstractBaseTest {

    public final static String ABV = "abv";
    public final static String ID = "id";
    public final static String NAME = "name";
    public final static String STYLE = "style";
    public final static String BREWERY_ID = "brewery_id";
    public final static Field[] FIELDS = new Field[]{Field.text(NAME).matcher(Field.Text.PhoneticMatcher.English).build(), Field.tag(STYLE).sortable().build(), Field.numeric(ABV).sortable().build(), Field.tag(BREWERY_ID).sortable().build()};
    public final static String BEERS = "beers";

    protected static RedisModulesClient client;
    protected static StatefulRedisModulesConnection<String, String> rediSearchConnection;
    protected static String host;
    protected static int port;
    private static RedisURI redisURI;

    @Container
    @SuppressWarnings("rawtypes")
    public static final RedisModulesContainer REDISEARCH = new RedisModulesContainer();

    @BeforeAll
    public static void setup() throws IOException {
        host = REDISEARCH.getHost();
        port = REDISEARCH.getFirstMappedPort();
        redisURI = RedisURI.create(host, port);
        client = RedisModulesClient.create(redisURI);
        rediSearchConnection = client.connect();
        RedisModulesCommands<String, String> sync = rediSearchConnection.sync();
        sync.flushall();
        List<Map<String, String>> beers = beers();
        sync.create(BEERS, CreateOptions.<String, String>builder().prefix("beer").build(), FIELDS);
        RedisModulesAsyncCommands<String, String> async = rediSearchConnection.async();
        async.setAutoFlushCommands(false);
        List<RedisFuture<?>> futures = new ArrayList<>();
        for (Map<String, String> beer : beers) {
            futures.add(async.hset("beer:" + beer.get(ID), beer));
        }
        async.flushCommands();
        async.setAutoFlushCommands(true);
        LettuceFutures.awaitAll(RedisURI.DEFAULT_TIMEOUT_DURATION, futures.toArray(new RedisFuture[0]));
    }

    @AfterAll
    protected static void teardown() {
        if (rediSearchConnection != null) {
            rediSearchConnection.close();
        }
        if (client != null) {
            client.shutdown();
        }
    }

    protected static List<Map<String, String>> beers() throws IOException {
        CsvSchema schema = CsvSchema.builder().setUseHeader(true).setNullValue("").build();
        CsvMapper mapper = new CsvMapper();
        InputStream inputStream = AbstractBaseTest.class.getClassLoader().getResourceAsStream("beers" + ".csv");
        MappingIterator<Map<String, String>> iterator = mapper.readerFor(Map.class).with(schema).readValues(inputStream);
        return iterator.readAll();
    }

    protected CalciteAssert.ConnectionFactory newConnectionFactory() {
        return new CalciteAssert.ConnectionFactory() {
            @Override
            public Connection createConnection() throws SQLException {
                return connection();
            }
        };
    }

    protected Connection connection() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:calcite:lex=JAVA");
        final SchemaPlus root = connection.unwrap(CalciteConnection.class).getRootSchema();
        root.add("redisearch", new RediSearchSchema(client.connect(), Arrays.asList(BEERS)));
        return connection;
    }

    protected CalciteAssert.AssertThat calciteAssert() {
        return CalciteAssert.that().with(newConnectionFactory());
    }

}
