package org.apache.calcite.adapter.redisearch;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.util.TestUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.lettucemod.search.CreateOptions;
import com.redis.lettucemod.search.Field;
import com.redis.lettucemod.search.Field.TextField.PhoneticMatcher;
import com.redis.testcontainers.RedisModulesContainer;

import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;

@Testcontainers
class RediSearchAdapterTest {

	public final static String ABV = "abv";
	public final static String ID = "id";
	public final static String NAME = "name";
	public final static String STYLE = "style";
	public final static String BREWERY_ID = "brewery_id";
	public final static Field[] FIELDS = new Field[] { Field.text(NAME).matcher(PhoneticMatcher.ENGLISH).build(),
			Field.tag(STYLE).sortable().build(), Field.numeric(ABV).sortable().build(),
			Field.tag(BREWERY_ID).sortable().build() };
	public final static String BEERS = "beers";

	protected static String host;
	protected static int port;

	@Container
	public static final RedisModulesContainer REDISEARCH = new RedisModulesContainer(
			RedisModulesContainer.DEFAULT_IMAGE_NAME.withTag(RedisModulesContainer.DEFAULT_TAG));

	@BeforeAll
	public static void setup() throws IOException {
		host = REDISEARCH.getHost();
		port = REDISEARCH.getFirstMappedPort();
		RedisModulesClient client = RedisModulesClient.create(RedisURI.create(host, port));
		StatefulRedisModulesConnection<String, String> connection = client.connect();
		RedisModulesCommands<String, String> sync = connection.sync();
		sync.flushall();
		List<Map<String, String>> beers = beers();
		sync.create(BEERS, CreateOptions.<String, String>builder().prefix("beer").build(), FIELDS);
		RedisModulesAsyncCommands<String, String> async = connection.async();
		async.setAutoFlushCommands(false);
		List<RedisFuture<?>> futures = new ArrayList<>();
		for (Map<String, String> beer : beers) {
			futures.add(async.hset("beer:" + beer.get(ID), beer));
		}
		async.flushCommands();
		async.setAutoFlushCommands(true);
		LettuceFutures.awaitAll(RedisURI.DEFAULT_TIMEOUT_DURATION, futures.toArray(new RedisFuture[0]));
		connection.close();
		client.shutdown();
		client.getResources().shutdown();
	}

	protected static List<Map<String, String>> beers() throws IOException {
		CsvSchema schema = CsvSchema.builder().setUseHeader(true).setNullValue("").build();
		CsvMapper mapper = new CsvMapper();
		InputStream inputStream = RediSearchAdapterTest.class.getClassLoader().getResourceAsStream("beers" + ".csv");
		MappingIterator<Map<String, String>> iterator = mapper.readerFor(Map.class).with(schema)
				.readValues(inputStream);
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
		root.add("redisearch",
				new RediSearchSchema(RedisModulesClient.create(REDISEARCH.getRedisURI()).connect(), BEERS));
		return connection;
	}

	protected CalciteAssert.AssertThat calciteAssert() {
		return CalciteAssert.that().with(newConnectionFactory());
	}

	@SuppressWarnings("rawtypes")
	public static Consumer<List> checker(final String... expected) {
		Objects.requireNonNull(expected, "string");
		return a -> {
			String actual = a == null || a.isEmpty() ? null : ((String) a.get(0));
			if (!expected[0].equals(actual)) {
				assertEquals(expected[0], actual, "expected and actual RediSearch queries do not match");
			}
		};
	}

	/**
	 * Tests using a Calcite view.
	 */
	@Test
	void view() {
		String expected = "name=Sinister; style=American Double / Imperial IPA; abv=0.09; brewery_id=177\nname=Sinister Minister Black IPA; style=American IPA; abv=0.077; brewery_id=133\n";
		calciteAssert().query("select * from redisearch.beers where name = 'Sinis*'").returns(expected).returnsCount(2);
	}

	@Test
	void emptyResult() {
		calciteAssert().query("select * from redisearch.beers limit 0").returnsCount(0);
	}

	@Test
	void basic() {
		String expected = "name=Pub Beer; style=American Pale Lager; abv=0.05; brewery_id=408\nname=Sinister; style=American Double / Imperial IPA; abv=0.09; brewery_id=177\nname=Sex and Candy; style=American IPA; abv=0.075; brewery_id=177\nname=Black Exodus; style=Oatmeal Stout; abv=0.077; brewery_id=177\nname=Lake Street Express; style=American Pale Ale (APA); abv=0.045; brewery_id=177\nname=Foreman; style=American Porter; abv=0.065; brewery_id=177\nname=Jade; style=American Pale Ale (APA); abv=0.055; brewery_id=177\nname=Cone Crusher; style=American Double / Imperial IPA; abv=0.086; brewery_id=177\nname=Sophomoric Saison; style=Saison / Farmhouse Ale; abv=0.07200000000000001; brewery_id=177\nname=Garce Selé; style=Saison / Farmhouse Ale; abv=0.069; brewery_id=177\nname=Troll Destroyer; style=Belgian IPA; abv=0.085; brewery_id=177\nname=Bitter Bitch; style=American Pale Ale (APA); abv=0.061; brewery_id=177\nname=Ginja Ninja; style=Cider; abv=0.06; brewery_id=154\nname=Cherried Away; style=Cider; abv=0.06; brewery_id=154\nname=Rhubarbarian; style=Cider; abv=0.06; brewery_id=154\nname=BrightCider; style=Cider; abv=0.06; brewery_id=154\nname=He Said Baltic-Style Porter; style=Baltic Porter; abv=0.08199999999999999; brewery_id=368\nname=He Said Belgian-Style Tripel; style=Tripel; abv=0.08199999999999999; brewery_id=368\nname=Fireside Chat; style=Winter Warmer; abv=0.079; brewery_id=368\nname=Bitter American; style=American Pale Ale (APA); abv=0.044000000000000004; brewery_id=368\nname=21st Amendment Watermelon Wheat Beer (2006); style=Fruit / Vegetable Beer; abv=0.049; brewery_id=368\nname=21st Amendment IPA (2006); style=American IPA; abv=0.07; brewery_id=368\nname=Special Edition: Allies Win The War!; style=English Strong Ale; abv=0.085; brewery_id=368\nname=Hop Crisis; style=American Double / Imperial IPA; abv=0.09699999999999999; brewery_id=368\nname=Bitter American (2011); style=American Pale Ale (APA); abv=0.044000000000000004; brewery_id=368\nname=Fireside Chat (2010); style=Winter Warmer; abv=0.079; brewery_id=368\nname=Bimini Twist; style=American IPA; abv=0.07; brewery_id=67\nname=Beach Blonde; style=American Blonde Ale; abv=0.05; brewery_id=67\nname=Passion Fruit Prussia; style=Berliner Weissbier; abv=0.035; brewery_id=60\nname=Send Help; style=American Blonde Ale; abv=0.045; brewery_id=60\nname=Cast Iron Oatmeal Brown; style=American Brown Ale; abv=0.055; brewery_id=60\nname=Reprise Centennial Red; style=American Amber / Red Ale; abv=0.06; brewery_id=60\nname=Alter Ego; style=American Black Ale; abv=0.055; brewery_id=60\nname=Divided Sky; style=American IPA; abv=0.065; brewery_id=60\nname=Resurrected; style=American IPA; abv=0.065; brewery_id=60\nname=Contact High; style=American Pale Wheat Ale; abv=0.05; brewery_id=60\nname=Galaxyfest; style=American IPA; abv=0.065; brewery_id=27\nname=Citrafest; style=American IPA; abv=0.05; brewery_id=27\nname=Barn Yeti; style=Belgian Strong Dark Ale; abv=0.09; brewery_id=27\nname=Scarecrow; style=American IPA; abv=0.069; brewery_id=27\nname=Ironman; style=English Strong Ale; abv=0.09; brewery_id=27\nname=Honey Kolsch; style=Kölsch; abv=0.046; brewery_id=27\nname=Copperhead Amber; style=Belgian Dark Ale; abv=0.052000000000000005; brewery_id=27\nname=Rude Parrot IPA; style=American IPA; abv=0.059000000000000004; brewery_id=481\nname=British Pale Ale (2010); style=English Pale Ale; abv=0.054000000000000006; brewery_id=481\nname=British Pale Ale; style=English Pale Ale; abv=0.054000000000000006; brewery_id=481\nname=Ballz Deep Double IPA; style=American Double / Imperial IPA; abv=0.084; brewery_id=481\nname=Colorado Native; style=American Amber / Red Lager; abv=0.055; brewery_id=462\nname=Colorado Native (2011); style=American Amber / Red Lager; abv=0.055; brewery_id=462\nname=Jockamo IPA; style=American IPA; abv=0.065; brewery_id=533\nname=Purple Haze; style=Fruit / Vegetable Beer; abv=0.042; brewery_id=533\nname=Abita Amber; style=American Amber / Red Lager; abv=0.045; brewery_id=533\nname=Citra Ass Down; style=American IPA; abv=0.08199999999999999; brewery_id=62\nname=The Brown Note; style=American Brown Ale; abv=0.05; brewery_id=62\nname=Citra Ass Down; style=American Double / Imperial IPA; abv=0.08; brewery_id=1\nname=London Balling; style=English Barleywine; abv=0.125; brewery_id=1\nname=35 K; style=Milk / Sweet Stout; abv=0.077; brewery_id=1\nname=A Beer; style=American Pale Ale (APA); abv=0.042; brewery_id=1\nname=Sho'nuff; style=Belgian Pale Ale; abv=0.04; brewery_id=1\nname=Bloody Show; style=American Pilsner; abv=0.055; brewery_id=1\nname=Rico Sauvin; style=American Double / Imperial IPA; abv=0.076; brewery_id=1\nname=Kamen Knuddeln; style=American Wild Ale; abv=0.065; brewery_id=1\nname=The Brown Note; style=English Brown Ale; abv=0.05; brewery_id=1\nname=Oatmeal PSA; style=American Pale Ale (APA); abv=0.05; brewery_id=367\nname=Pre Flight Pilsner; style=American Pilsner; abv=0.052000000000000005; brewery_id=367\nname=P-Town Pilsner; style=American Pilsner; abv=0.04; brewery_id=117\nname=Klickitat Pale Ale; style=American Pale Ale (APA); abv=0.053; brewery_id=117\nname=Yellow Wolf Imperial IPA; style=American Double / Imperial IPA; abv=0.08199999999999999; brewery_id=117\nname=Freeride APA; style=American Pale Ale (APA); abv=0.053; brewery_id=270\nname=Alaskan Amber; style=Altbier; abv=0.053; brewery_id=270\nname=Hopalicious; style=American Pale Ale (APA); abv=0.057; brewery_id=73\nname=Kentucky Kölsch; style=Kölsch; abv=0.043; brewery_id=388\nname=Kentucky IPA; style=American IPA; abv=0.065; brewery_id=388\nname=Dusty Trail Pale Ale; style=American Pale Ale (APA); abv=0.054000000000000006; brewery_id=401\nname=Damnesia; style=American IPA; abv=0.062; brewery_id=401\nname=Desolation IPA; style=American IPA; abv=0.062; brewery_id=401\nname=Liberty Ale; style=American IPA; abv=0.059000000000000004; brewery_id=35\nname=IPA; style=American IPA; abv=0.065; brewery_id=35\nname=Summer Wheat; style=American Pale Wheat Ale; abv=0.045; brewery_id=35\nname=California Lager; style=American Amber / Red Lager; abv=0.049; brewery_id=35\nname=Brotherhood Steam; style=California Common / Steam Beer; abv=0.055999999999999994; brewery_id=35\nname=Blood Orange Gose; style=Gose; abv=0.042; brewery_id=171\nname=Keebarlin' Pale Ale; style=American Pale Ale (APA); abv=0.042; brewery_id=171\nname=Fall Hornin'; style=Pumpkin Ale; abv=0.06; brewery_id=171\nname=Barney Flats Oatmeal Stout; style=Oatmeal Stout; abv=0.057; brewery_id=171\nname=Summer Solstice; style=Cream Ale; abv=0.055999999999999994; brewery_id=171\nname=Hop Ottin' IPA; style=American IPA; abv=0.07; brewery_id=171\nname=Boont Amber Ale; style=American Amber / Red Ale; abv=0.057999999999999996; brewery_id=171\nname=Barney Flats Oatmeal Stout; style=Oatmeal Stout; abv=0.057; brewery_id=171\nname=El Steinber Dark Lager; style=Vienna Lager; abv=0.055; brewery_id=171\nname=Boont Amber Ale (2010); style=American Amber / Red Ale; abv=0.057999999999999996; brewery_id=171\nname=Summer Solstice Cerveza Crema (2009); style=Cream Ale; abv=0.055999999999999994; brewery_id=171\nname=Barney Flats Oatmeal Stout (2012); style=Oatmeal Stout; abv=0.057; brewery_id=171\nname=Winter Solstice; style=Winter Warmer; abv=0.069; brewery_id=171\nname=Hop Ottin' IPA (2011); style=American IPA; abv=0.07; brewery_id=171\nname=Boont Amber Ale (2011); style=American Amber / Red Ale; abv=0.057999999999999996; brewery_id=171\nname=Summer Solstice (2011); style=Cream Ale; abv=0.055999999999999994; brewery_id=171\nname=Poleeko Gold Pale Ale (2009); style=American Pale Ale (APA); abv=0.055; brewery_id=171\nname=River Pig Pale Ale; style=American Pale Ale (APA); abv=0.054000000000000006; brewery_id=542\nname=Angry Orchard Apple Ginger; style=Cider; abv=0.05; brewery_id=434\n";
		calciteAssert().query("select * from redisearch.beers").returns(expected);
	}

	@Test
	void filter() {
		calciteAssert().query("select * from redisearch.beers where name = 'Sinister'").returnsCount(4);
		calciteAssert().query("select * from redisearch.beers where name in ('SINISTER', 'FOREMAN')").returnsCount(11);
		calciteAssert().query("select * from redisearch.beers limit 0").returnsCount(0);
	}

	@Test
	void testSort() {
		final String explain = "PLAN=RediSearchToEnumerableConverter\n  RediSearchSort(sort0=[$2], dir0=[ASC])\n    RediSearchTableScan(table=[[redisearch, beers]])\n\n";
		calciteAssert().query("select * from redisearch.beers order by abv").returnsCount(RediSearchTable.DEFAULT_LIMIT)
				.returns(sortedResultSetChecker("abv", RelFieldCollation.Direction.ASCENDING)).explainContains(explain);
	}

	@Test
	void testSortLimit() {
		final String sql = "select name, abv from redisearch.beers\n" + "order by abv offset 2 rows " + "fetch"
				+ " next 3 " + "rows" + " only";
		String expected = "name=American Light; abv=0.032\nname=Bikini Beer; abv=0.027000000000000003\nname=Summer Brew; abv=0.027999999999999997";
		calciteAssert().query(sql).returnsUnordered(expected).queryContains(checker("*"));
	}

	/**
	 * Throws {@code AssertionError} if result set is not sorted by {@code column}.
	 * {@code null}s are ignored.
	 *
	 * @param column    column to be extracted (as comparable object).
	 * @param direction ascending / descending
	 * @return consumer which throws exception
	 */
	private static Consumer<ResultSet> sortedResultSetChecker(String column, RelFieldCollation.Direction direction) {
		Objects.requireNonNull(column, "column");
		return rset -> {
			try {
				final List<Comparable<?>> result = new ArrayList<>();
				while (rset.next()) {
					Object object = rset.getObject(column);
					if (object != null && !(object instanceof Comparable)) {
						final String message = String.format(Locale.ROOT, "%s is not comparable", object);
						throw new IllegalStateException(message);
					}
					if (object != null) {
						// noinspection rawtypes
						result.add((Comparable) object);
					}
				}
				for (int i = 0; i < result.size() - 1; i++) {
					// noinspection rawtypes
					final Comparable current = result.get(i);
					// noinspection rawtypes
					final Comparable next = result.get(i + 1);
					// noinspection unchecked
					final int cmp = current.compareTo(next);
					if (direction == RelFieldCollation.Direction.ASCENDING ? cmp > 0 : cmp < 0) {
						final String message = String.format(Locale.ROOT,
								"Column %s NOT sorted (%s): %s " + "(index:%d) > %s (index:%d) count: %d", column,
								direction, current, i, next, i + 1, result.size());
						throw new AssertionError(message);
					}
				}
			} catch (SQLException e) {
				throw TestUtil.rethrow(e);
			}
		};
	}

	/**
	 * Sorting (and aggregating) directly on items without a view.
	 */
	@Test
	void testSortNoSchema() {
		CalciteAssert.that().with(newConnectionFactory()).query("select * from redisearch.beers order by name")
				.returnsCount(RediSearchTable.DEFAULT_LIMIT);
		CalciteAssert.that().with(newConnectionFactory()).query("select style from redisearch.beers order by name")
				.returnsCount(RediSearchTable.DEFAULT_LIMIT);
		String expected = "name=#002 American I.P.A.\nname=#004 Session I.P.A.\nname=113 IPA\nname=11th Hour IPA\nname=1916 Shore Shiver\nname=2014 IPA Cicada Series\nname=2020 IPA\nname=21st Amendment IPA (2006)\nname=3-Way IPA (2013)\nname=360° India Pale Ale\nname=3:33 Black IPA\nname=40 Mile IPA\nname=4000 Footer IPA\nname=5 Day IPA\nname=51K IPA\nname=77 Fremont Select Spring Session IPA\nname=98 Problems (Cuz A Hop Ain't One)\nname=Ace IPA\nname=All Day IPA\nname=Alloy\nname=Almanac IPA\nname=Alphadelic IPA\nname=Alphadelic IPA (2011)\nname=Amazon Princess IPA\nname=Ananda India Pale Ale\nname=Anti-Hero IPA\nname=Arcus IPA\nname=Aslan IPA\nname=Autumnation (2013)\nname=Avery India Pale Ale\nname=Baby Daddy Session IPA\nname=Back Bay IPA\nname=Backyahd IPA\nname=Bark Bite IPA\nname=Barrio Blanco\nname=Batch 69 IPA\nname=Be Hoppy IPA\nname=Bengali\nname=Bengali Tiger\nname=Bengali Tiger (2011)\nname=Bent Hop Golden IPA\nname=Better Half\nname=Better Weather IPA\nname=Beyond The Pale IPA\nname=Big Cock IPA\nname=Big Elm IPA\nname=Big Eye India Pale Ale\nname=Big Nose\nname=Big Sky IPA\nname=Big Sky IPA (2012)\nname=Big Swell IPA\nname=Bikini Beer\nname=Bimini Twist\nname=Blackmarket Rye IPA\nname=Blue Boots IPA\nname=Blur India Pale Ale\nname=Boat Beer\nname=Booming Rollers\nname=Bozone HopZone IPA\nname=Brew Free! or Die IPA\nname=Brew Free! or Die IPA (2008)\nname=Brew Free! or Die IPA (2009)\nname=Burning Bush Smoked IPA\nname=Caldera IPA\nname=Caldera IPA (2007)\nname=Caldera IPA (2009)\nname=California Sunshine Rye IPA\nname=Camelback\nname=Campside Session IPA\nname=Category 3 IPA\nname=Centennial IPA\nname=Charlie in the Rye\nname=Charlie's Rye IPA\nname=Chupahopra\nname=Citra Ass Down\nname=Citrafest\nname=City of the Sun\nname=Clean Shave IPA\nname=Cornerstone IPA\nname=Country Boy IPA\nname=County Line IPA\nname=Crank Yanker IPA\nname=Crank Yanker IPA (2011)\nname=Cropduster Mid-American IPA\nname=Dagger Falls IPA\nname=Dagger Falls IPA\nname=Dagger Falls IPA\nname=Damnesia\nname=Dank IPA\nname=Dank IPA (2012)\nname=Dankosaurus\nname=Day Hike Session\nname=Dayman IPA\nname=Deep Ellum IPA\nname=Des Moines IPA\nname=Descender IPA\nname=Desert Magic IPA\nname=Desolation IPA\nname=Disco Superfly\nname=Divided Sky";
		CalciteAssert.that().with(newConnectionFactory())
				.query("select name from redisearch.beers where style = 'American IPA' order by name")
				.returnsOrdered(expected);
	}

	@Test
	void testFilterSort() {
		final String sql = "select * from redisearch.beers where style = 'American IPA' and abv >= .05 order by abv";
		final String explain = "PLAN=RediSearchToEnumerableConverter\n  RediSearchSort(sort0=[$2], dir0=[ASC])\n    RediSearchFilter(condition=[AND(=($1, 'American IPA'), >=($2, 0.05:DECIMAL(2, 2)))])\n      RediSearchTableScan(table=[[redisearch, beers]])\n\n";
		final String expected = "name=Hop Session; style=American IPA; abv=0.05; brewery_id=47\nname=South Bay Session IPA; style=American IPA; abv=0.05; brewery_id=33\nname=TailGate IPA; style=American IPA; abv=0.05; brewery_id=449\nname=TailGate IPA; style=American IPA; abv=0.05; brewery_id=449\nname=Dayman IPA; style=American IPA; abv=0.05; brewery_id=43\nname=Jah Mon; style=American IPA; abv=0.05; brewery_id=43\nname=Lil SIPA; style=American IPA; abv=0.05; brewery_id=321\nname=Grapefruit IPA; style=American IPA; abv=0.05; brewery_id=13\nname=Lasso; style=American IPA; abv=0.05; brewery_id=6\nname=Grand Circus IPA; style=American IPA; abv=0.05; brewery_id=72\nname=Citrafest; style=American IPA; abv=0.05; brewery_id=27\nname=Quaff India Style Session Ale; style=American IPA; abv=0.051; brewery_id=201\nname=Self Starter; style=American IPA; abv=0.052000000000000005; brewery_id=94\nname=Firewater India Pale Ale; style=American IPA; abv=0.052000000000000005; brewery_id=331\nname=Firewater India Pale Ale; style=American IPA; abv=0.052000000000000005; brewery_id=331\nname=Nice Rack IPA; style=American IPA; abv=0.055; brewery_id=436\nname=Texas Pale Ale (TPA); style=American IPA; abv=0.055; brewery_id=257\nname=Pump House IPA; style=American IPA; abv=0.055; brewery_id=68\nname=Manayunk IPA; style=American IPA; abv=0.055; brewery_id=356\nname=Mosaic Single Hop IPA; style=American IPA; abv=0.055; brewery_id=41\nname=Fisherman's IPA; style=American IPA; abv=0.055; brewery_id=230\nname=Wachusett IPA; style=American IPA; abv=0.055999999999999994; brewery_id=295\nname=Duluchan India Pale Ale; style=American IPA; abv=0.055999999999999994; brewery_id=345\nname=Ghost Ship White IPA; style=American IPA; abv=0.055999999999999994; brewery_id=192\nname=Triangle India Pale Ale; style=American IPA; abv=0.057; brewery_id=524\nname=Sockeye Red IPA; style=American IPA; abv=0.057; brewery_id=223\nname=Tumbleweed IPA; style=American IPA; abv=0.057; brewery_id=537\nname=IPA #11; style=American IPA; abv=0.057; brewery_id=121\nname=Train Hopper; style=American IPA; abv=0.057999999999999996; brewery_id=14\nname=Charlie in the Rye; style=American IPA; abv=0.057999999999999996; brewery_id=351\nname=Alloy; style=American IPA; abv=0.057999999999999996; brewery_id=17\nname=Hop Farm IPA; style=American IPA; abv=0.057999999999999996; brewery_id=297\nname=Mango Ginger; style=American IPA; abv=0.057999999999999996; brewery_id=167\nname=Troopers Alley IPA; style=American IPA; abv=0.059000000000000004; brewery_id=344\nname=Harpoon IPA (2010); style=American IPA; abv=0.059000000000000004; brewery_id=234\nname=Harpoon IPA (2012); style=American IPA; abv=0.059000000000000004; brewery_id=234\nname=Harpoon IPA; style=American IPA; abv=0.059000000000000004; brewery_id=234\nname=Goose Island India Pale Ale; style=American IPA; abv=0.059000000000000004; brewery_id=196\nname=Point the Way IPA (2012); style=American IPA; abv=0.059000000000000004; brewery_id=240\nname=Point the Way IPA; style=American IPA; abv=0.059000000000000004; brewery_id=240\nname=Point the Way IPA; style=American IPA; abv=0.059000000000000004; brewery_id=240\nname=Soul Doubt; style=American IPA; abv=0.059000000000000004; brewery_id=66\nname=Liberty Ale; style=American IPA; abv=0.059000000000000004; brewery_id=35\nname=Rude Parrot IPA; style=American IPA; abv=0.059000000000000004; brewery_id=481\nname=Super G IPA; style=American IPA; abv=0.06; brewery_id=396\nname=Green Monsta IPA; style=American IPA; abv=0.06; brewery_id=295\nname=Green Monsta IPA; style=American IPA; abv=0.06; brewery_id=295\nname=Dragonfly IPA; style=American IPA; abv=0.06; brewery_id=202\nname=The Green Room; style=American IPA; abv=0.06; brewery_id=126\nname=40 Mile IPA; style=American IPA; abv=0.06; brewery_id=273\nname=Homefront IPA; style=American IPA; abv=0.06; brewery_id=163\nname=Saranac White IPA; style=American IPA; abv=0.06; brewery_id=299\nname=King Street IPA; style=American IPA; abv=0.06; brewery_id=102\nname=KelSo India Pale Ale; style=American IPA; abv=0.06; brewery_id=342\nname=JP's Ould Sod Irish Red IPA; style=American IPA; abv=0.06; brewery_id=32\nname=Good People IPA; style=American IPA; abv=0.06; brewery_id=478\nname=The 26th; style=American IPA; abv=0.06; brewery_id=22\nname=Backyahd IPA; style=American IPA; abv=0.06; brewery_id=279\nname=Half Cycle IPA; style=American IPA; abv=0.06; brewery_id=16\nname=11th Hour IPA; style=American IPA; abv=0.06; brewery_id=212\nname=Linnaeus Mango IPA; style=American IPA; abv=0.06; brewery_id=10\nname=House Brand IPA; style=American IPA; abv=0.06; brewery_id=519\nname=Barrio Blanco; style=American IPA; abv=0.06; brewery_id=251\nname=Shiva IPA; style=American IPA; abv=0.06; brewery_id=528\nname=Charlie's Rye IPA; style=American IPA; abv=0.06; brewery_id=146\nname=Pile of Face; style=American IPA; abv=0.06; brewery_id=1\nname=Camelback; style=American IPA; abv=0.061; brewery_id=157\nname=5 Day IPA; style=American IPA; abv=0.061; brewery_id=442\nname=Lumberyard IPA; style=American IPA; abv=0.061; brewery_id=158\nname=Category 3 IPA; style=American IPA; abv=0.061; brewery_id=340\nname=Caldera IPA; style=American IPA; abv=0.061; brewery_id=155\nname=Caldera IPA (2007); style=American IPA; abv=0.061; brewery_id=155\nname=Caldera IPA (2009); style=American IPA; abv=0.061; brewery_id=155\nname=Fairweather IPA; style=American IPA; abv=0.061; brewery_id=493\nname=Ananda India Pale Ale; style=American IPA; abv=0.062; brewery_id=239\nname=Evolutionary IPA (2011); style=American IPA; abv=0.062; brewery_id=190\nname=Evolutionary IPA (2012); style=American IPA; abv=0.062; brewery_id=190\nname=Evo IPA; style=American IPA; abv=0.062; brewery_id=190\nname=Hoppy Boy; style=American IPA; abv=0.062; brewery_id=520\nname=Almanac IPA; style=American IPA; abv=0.062; brewery_id=265\nname=Hoodoo Voodoo IPA; style=American IPA; abv=0.062; brewery_id=153\nname=Furious; style=American IPA; abv=0.062; brewery_id=61\nname=Spiteful IPA; style=American IPA; abv=0.062; brewery_id=175\nname=360° India Pale Ale; style=American IPA; abv=0.062; brewery_id=371\nname=Old Wylie's IPA; style=American IPA; abv=0.062; brewery_id=43\nname=Gangway IPA; style=American IPA; abv=0.062; brewery_id=475\nname=Amazon Princess IPA; style=American IPA; abv=0.062; brewery_id=205\nname=Outlaw IPA; style=American IPA; abv=0.062; brewery_id=307\nname=Sea Hag IPA (Current); style=American IPA; abv=0.062; brewery_id=410\nname=Sea Hag IPA; style=American IPA; abv=0.062; brewery_id=410\nname=Big Swell IPA; style=American IPA; abv=0.062; brewery_id=375\nname=Marble India Pale Ale; style=American IPA; abv=0.062; brewery_id=443\nname=Face Plant IPA; style=American IPA; abv=0.062; brewery_id=430\nname=Great Crescent IPA (2011); style=American IPA; abv=0.062; brewery_id=165\nname=Great Crescent IPA; style=American IPA; abv=0.062; brewery_id=165\nname=The Optimist; style=American IPA; abv=0.062; brewery_id=206\nname=Country Boy IPA; style=American IPA; abv=0.062; brewery_id=170\nname=Escape to Colorado; style=American IPA; abv=0.062; brewery_id=81\nname=Operation Homefront; style=American IPA; abv=0.062; brewery_id=141\nname=Mutiny IPA; style=American IPA; abv=0.062; brewery_id=192";
		final String query = "(@style:{American IPA} @abv:[0.05 inf])";
		calciteAssert().query(sql).returnsOrdered(expected).queryContains(checker(query)).explainContains(explain);
	}

	@Test
	void testBeers() {
		calciteAssert().query("select name, style from redisearch.beers").returnsCount(RediSearchTable.DEFAULT_LIMIT);
	}

	@Test
	void testFilterReversed() {
		String expected = "name=Csar; abv=0.12\nname=Lee Hill Series Vol. 4 - Manhattan Style Rye Ale; abv=0.10400000000000001";
		calciteAssert().query("select name, abv from redisearch.beers where .1 < abv order by name").limit(2)
				.returnsUnordered(expected);
		calciteAssert().query("select name, abv from redisearch.beers where abv > .1 order by name").limit(2)
				.returnsUnordered(expected);
	}

}
