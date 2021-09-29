package com.redis.calcite;

import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.util.TestUtil;

import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Consumer;

class RediSearchAdapterTest extends AbstractBaseTest {

    /**
     * Tests using a Calcite view.
     */
    @Test
    void view() {
        calciteAssert().query("select * from redisearch.beers where name = 'Sinis*'").returns("name=Sinister Minister Black IPA; style=American IPA; abv=0.077; brewery_id=133\nname=Sinister; style=American Double / Imperial IPA; abv=0.09; brewery_id=177\n").returnsCount(2);
    }

    @Test
    void emptyResult() {
        CalciteAssert.that().with(newConnectionFactory()).query("select * from redisearch.beers limit 0").returnsCount(0);
    }

    @Test
    void basic() {
        String expected = "name=Rail Yard Ale (2009); style=American Amber / Red Ale; abv=0.052000000000000005; brewery_id=424\nname=Silverback Pale Ale; style=American Pale Ale (APA); abv=0.055; brewery_id=424\nname=B3K Black Lager; style=Schwarzbier; abv=0.055; brewery_id=424\nname=Rail Yard Ale; style=American Amber / Red Ale; abv=0.052000000000000005; brewery_id=424\nname=Belgorado; style=Belgian IPA; abv=0.067; brewery_id=424\nname=Rocky Mountain Oyster Stout; style=American Stout; abv=0.075; brewery_id=424\nname=Wynkoop Pumpkin Ale; style=Pumpkin Ale; abv=0.055; brewery_id=424\nname=Colorojo Imperial Red Ale; style=American Strong Ale; abv=0.08199999999999999; brewery_id=424\nname=Worthy Pale; style=American Pale Ale (APA); abv=0.06; brewery_id=199\nname=Worthy IPA (2013); style=American IPA; abv=0.069; brewery_id=199\nname=Lights Out Vanilla Cream Extra Stout; style=American Double / Imperial IPA; abv=0.077; brewery_id=199\nname=Easy Day Kolsch; style=Kölsch; abv=0.045; brewery_id=199\nname=Worthy IPA; style=American IPA; abv=0.069; brewery_id=199\nname=Be Hoppy IPA; style=American IPA; abv=0.065; brewery_id=339\nname=Summer Brew; style=American Pilsner; abv=0.027999999999999997; brewery_id=109\nname=4000 Footer IPA; style=American IPA; abv=0.065; brewery_id=109\nname=Woodchuck Amber Hard Cider; style=Cider; abv=0.05; brewery_id=501\nname=Wolverine Premium Lager; style=American Pale Lager; abv=0.047; brewery_id=402\nname=Troopers Alley IPA; style=American IPA; abv=0.059000000000000004; brewery_id=344\nname=Edward’s Portly Brown; style=American Brown Ale; abv=0.045; brewery_id=14\nname=Train Hopper; style=American IPA; abv=0.057999999999999996; brewery_id=14\nname=Tiny Bomb; style=American Pilsner; abv=0.045; brewery_id=239\nname=Ananda India Pale Ale; style=American IPA; abv=0.062; brewery_id=239\nname=Tarasque; style=Saison / Farmhouse Ale; abv=0.059000000000000004; brewery_id=239\nname=#003 Brown & Robust Porter; style=American Porter; abv=0.052000000000000005; brewery_id=211\nname=#001 Golden Amber Lager; style=American Amber / Red Lager; abv=0.055; brewery_id=211\nname=P-51 Porter; style=American Porter; abv=0.08; brewery_id=509\nname=Ace IPA; style=American IPA; abv=0.07400000000000001; brewery_id=509\nname=Wind River Blonde Ale; style=American Blonde Ale; abv=0.05; brewery_id=550\nname=Wyoming Pale Ale; style=American Pale Ale (APA); abv=0.07200000000000001; brewery_id=550\nname=Ambitious Lager; style=Munich Helles Lager; abv=0.05; brewery_id=499\nname=Bodacious Bock; style=Bock; abv=0.075; brewery_id=499\nname=Mystical Stout; style=Irish Dry Stout; abv=0.054000000000000006; brewery_id=499\nname=Alpha Ale; style=American Pale Ale (APA); abv=0.051; brewery_id=181\nname=Wild Wolf American Pilsner; style=American Pilsner; abv=0.045; brewery_id=181\nname=Wild Wolf Wee Heavy Scottish Style Ale; style=Scotch Ale / Wee Heavy; abv=0.057; brewery_id=181\nname=Blonde Hunny; style=Belgian Pale Ale; abv=0.068; brewery_id=181\nname=Paddy Pale Ale; style=American Pale Ale (APA); abv=0.055999999999999994; brewery_id=361\nname=Wild Onion Pumpkin Ale (2010); style=Pumpkin Ale; abv=0.045; brewery_id=361\nname=Jack Stout; style=Oatmeal Stout; abv=0.06; brewery_id=361\nname=Wild Onion Summer Wit; style=Witbier; abv=0.042; brewery_id=361\nname=Hop Slayer Double IPA (2011); style=American Double / Imperial IPA; abv=0.08199999999999999; brewery_id=361\nname=Hop Slayer Double IPA (2011); style=American Double / Imperial IPA; abv=0.08199999999999999; brewery_id=361\nname=Phat Chance; style=American Blonde Ale; abv=0.052000000000000005; brewery_id=361\nname=Big Bowl Blonde Ale; style=American Brown Ale; abv=0.05; brewery_id=361\nname=Pumpkin Ale; style=Pumpkin Ale; abv=0.045; brewery_id=361\nname=Hop Slayer Double IPA; style=American Double / Imperial IPA; abv=0.08199999999999999; brewery_id=361\nname=Widmer Brothers Hefeweizen; style=Hefeweizen; abv=0.049; brewery_id=296\nname=Hefe Black; style=Hefeweizen; abv=0.049; brewery_id=296\nname=Hefe Lemon; style=Radler; abv=0.049; brewery_id=296\nname=Berliner Weisse; style=Berliner Weissbier; abv=0.055; brewery_id=47\nname=Blueberry Berliner Weisse; style=Berliner Weissbier; abv=0.055; brewery_id=47\nname=Hop Session; style=American IPA; abv=0.05; brewery_id=47\nname=Raspberry Berliner Weisse; style=Berliner Weissbier; abv=0.055; brewery_id=47\nname=Drop Kick Ale; style=American Amber / Red Ale; abv=0.052000000000000005; brewery_id=132\nname=O’Malley’s IPA; style=American IPA; abv=0.075; brewery_id=132\nname=Rip Van Winkle (Current); style=Bock; abv=0.08; brewery_id=132\nname=Charlie in the Rye; style=American IPA; abv=0.057999999999999996; brewery_id=351\nname=Westfield Octoberfest; style=Märzen / Oktoberfest; abv=0.057; brewery_id=351\nname=Westbrook IPA; style=American IPA; abv=0.068; brewery_id=384\nname=White Thai; style=Witbier; abv=0.05; brewery_id=384\nname=Westbrook Gose; style=Gose; abv=0.04; brewery_id=384\nname=One Claw; style=American Pale Ale (APA); abv=0.055; brewery_id=384\nname=West Sixth Amber Ale; style=American Amber / Red Ale; abv=0.055; brewery_id=100\nname=Pay It Forward Cocoa Porter; style=American Porter; abv=0.07; brewery_id=100\nname=Christmas Ale; style=Herbed / Spiced Beer; abv=0.09; brewery_id=100\nname=Flyin' Rye; style=American IPA; abv=0.07; brewery_id=94\nname=10 Ton; style=Oatmeal Stout; abv=0.07; brewery_id=94\nname=Self Starter; style=American IPA; abv=0.052000000000000005; brewery_id=94\nname=T-6 Red Ale (2004); style=American Amber / Red Ale; abv=0.047; brewery_id=506\nname=Green Monsta IPA; style=American IPA; abv=0.06; brewery_id=295\nname=Wachusett Blueberry Ale; style=Fruit / Vegetable Beer; abv=0.045; brewery_id=295\nname=Pumpkan; style=Pumpkin Ale; abv=0.052000000000000005; brewery_id=295\nname=Wachusett Light IPA (2013); style=American IPA; abv=0.04; brewery_id=295\nname=Country Pale Ale; style=English Pale Ale; abv=0.051; brewery_id=295\nname=Wachusett Summer; style=American Pale Wheat Ale; abv=0.047; brewery_id=295\nname=Larry Imperial IPA; style=American Double / Imperial IPA; abv=0.085; brewery_id=295\nname=Strawberry White; style=Witbier; abv=0.047; brewery_id=295\nname=Wachusett IPA; style=American IPA; abv=0.055999999999999994; brewery_id=295\nname=Green Monsta IPA; style=American IPA; abv=0.06; brewery_id=295\nname=Wachusett Light IPA; style=American IPA; abv=0.04; brewery_id=295\nname=Pilzilla; style=American Double / Imperial Pilsner; abv=0.075; brewery_id=322\nname=Good Vibes IPA; style=American IPA; abv=0.073; brewery_id=322\nname=Gran Met; style=Belgian Strong Pale Ale; abv=0.092; brewery_id=322\nname=White Magick of the Sun; style=Witbier; abv=0.079; brewery_id=322\nname=Voodoo Love Child; style=Tripel; abv=0.092; brewery_id=322\nname=Nitro Can Coffee Stout; style=American Stout; abv=0.052000000000000005; brewery_id=113\nname=Hard Apple; style=Cider; abv=0.068; brewery_id=185\nname=Blue Gold; style=Cider; abv=0.068; brewery_id=185\nname=Totally Roasted; style=Cider; abv=0.068; brewery_id=185\nname=Ginger Peach; style=Cider; abv=0.069; brewery_id=185\nname=Nunica Pine; style=Cider; abv=0.068; brewery_id=185\nname=Squatters Full Suspension Pale Ale; style=American Pale Ale (APA); abv=0.04; brewery_id=302\nname=Squatters Hop Rising Double IPA (2014); style=American Double / Imperial IPA; abv=0.09; brewery_id=302\nname=Wasatch Apricot Hefeweizen; style=Fruit / Vegetable Beer; abv=0.04; brewery_id=302\nname=Wasatch Ghostrider White IPA (2014); style=American White IPA; abv=0.06; brewery_id=302\nname=Wasatch Ghostrider White IPA; style=American White IPA; abv=0.06; brewery_id=302\nname=Devastator Double Bock; style=Doppelbock; abv=0.08; brewery_id=302\nname=Squatters Hop Rising Double IPA; style=American Double / Imperial IPA; abv=0.09; brewery_id=302\nname=Squatters Full Suspension Pale Ale; style=American Pale Ale (APA); abv=0.04; brewery_id=302\n";
        CalciteAssert.that().with(newConnectionFactory()).query("select * from redisearch.beers").returns(expected);
    }

    @Test
    void filter() {
        CalciteAssert.that().with(newConnectionFactory()).query("select * from redisearch.beers where name = 'Sinister'").returnsCount(4);
        CalciteAssert.that().with(newConnectionFactory()).query("select * from redisearch.beers where name in ('SINISTER', 'FOREMAN')").returnsCount(11);
        CalciteAssert.that().with(newConnectionFactory()).query("select * from redisearch.beers limit 0").returnsCount(0);
    }

    @Test
    void testSort() {
        final String explain = "PLAN=RediSearchToEnumerableConverter\n  RediSearchSort(sort0=[$2], dir0=[ASC])\n    RediSearchTableScan(table=[[redisearch, beers]])\n\n";
        calciteAssert().query("select * from redisearch.beers order by abv").returnsCount(RediSearchTable.DEFAULT_LIMIT).returns(sortedResultSetChecker("abv", RelFieldCollation.Direction.ASCENDING)).explainContains(explain);
    }

    @Test
    void testSortLimit() {
        final String sql = "select name, abv from redisearch.beers\n" + "order by abv offset 2 rows " + "fetch" + " next 3 " + "rows" + " only";
        calciteAssert().query(sql).returnsUnordered("name=Rocket Girl; abv=0.032\nname=Summer Brew; " + "abv=0.027999999999999997\nname=Totally Radler; abv=0.027000000000000003").queryContains(RediSearchChecker.rediSearchChecker("*"));
    }

    /**
     * Throws {@code AssertionError} if result set is not sorted by {@code column}.
     * {@code null}s are ignored.
     *
     * @param column    column to be extracted (as comparable object).
     * @param direction ascending / descending
     * @return consumer which throws exception
     */
    @SuppressWarnings("SameParameterValue")
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
                        //noinspection rawtypes
                        result.add((Comparable) object);
                    }
                }
                for (int i = 0; i < result.size() - 1; i++) {
                    //noinspection rawtypes
                    final Comparable current = result.get(i);
                    //noinspection rawtypes
                    final Comparable next = result.get(i + 1);
                    //noinspection unchecked
                    final int cmp = current.compareTo(next);
                    if (direction == RelFieldCollation.Direction.ASCENDING ? cmp > 0 : cmp < 0) {
                        final String message = String.format(Locale.ROOT, "Column %s NOT sorted (%s): %s " + "(index:%d) > %s (index:%d) count: %d", column, direction, current, i, next, i + 1, result.size());
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
        CalciteAssert.that().with(newConnectionFactory()).query("select * from redisearch.beers order by name").returnsCount(RediSearchTable.DEFAULT_LIMIT);
        CalciteAssert.that().with(newConnectionFactory()).query("select style from redisearch.beers order by name").returnsCount(RediSearchTable.DEFAULT_LIMIT);
        String expected = "name=#002 American I.P.A.\nname=#004 Session I.P.A.\nname=113 IPA\nname=11th Hour IPA\nname=1916 Shore Shiver\nname=2014 IPA Cicada Series\nname=2020 IPA\nname=21st Amendment IPA (2006)\nname=3-Way IPA (2013)\nname=360° India Pale Ale\nname=3:33 Black IPA\nname=40 Mile IPA\nname=4000 Footer IPA\nname=5 Day IPA\nname=51K IPA\nname=77 Fremont Select Spring Session IPA\nname=98 Problems (Cuz A Hop Ain't One)\nname=Ace IPA\nname=All Day IPA\nname=Alloy\nname=Almanac IPA\nname=Alphadelic IPA\nname=Alphadelic IPA (2011)\nname=Amazon Princess IPA\nname=Ananda India Pale Ale\nname=Anti-Hero IPA\nname=Arcus IPA\nname=Aslan IPA\nname=Autumnation (2013)\nname=Avery India Pale Ale\nname=Baby Daddy Session IPA\nname=Back Bay IPA\nname=Backyahd IPA\nname=Bark Bite IPA\nname=Barrio Blanco\nname=Batch 69 IPA\nname=Be Hoppy IPA\nname=Bengali\nname=Bengali Tiger\nname=Bengali Tiger (2011)\nname=Bent Hop Golden IPA\nname=Better Half\nname=Better Weather IPA\nname=Beyond The Pale IPA\nname=Big Cock IPA\nname=Big Elm IPA\nname=Big Eye India Pale Ale\nname=Big Nose\nname=Big Sky IPA\nname=Big Sky IPA (2012)\nname=Big Swell IPA\nname=Bikini Beer\nname=Bimini Twist\nname=Blackmarket Rye IPA\nname=Blue Boots IPA\nname=Blur India Pale Ale\nname=Boat Beer\nname=Booming Rollers\nname=Bozone HopZone IPA\nname=Brew Free! or Die IPA\nname=Brew Free! or Die IPA (2008)\nname=Brew Free! or Die IPA (2009)\nname=Burning Bush Smoked IPA\nname=Caldera IPA\nname=Caldera IPA (2007)\nname=Caldera IPA (2009)\nname=California Sunshine Rye IPA\nname=Camelback\nname=Campside Session IPA\nname=Category 3 IPA\nname=Centennial IPA\nname=Charlie in the Rye\nname=Charlie's Rye IPA\nname=Chupahopra\nname=Citra Ass Down\nname=Citrafest\nname=City of the Sun\nname=Clean Shave IPA\nname=Cornerstone IPA\nname=Country Boy IPA\nname=County Line IPA\nname=Crank Yanker IPA\nname=Crank Yanker IPA (2011)\nname=Cropduster Mid-American IPA\nname=Dagger Falls IPA\nname=Dagger Falls IPA\nname=Dagger Falls IPA\nname=Damnesia\nname=Dank IPA\nname=Dank IPA (2012)\nname=Dankosaurus\nname=Day Hike Session\nname=Dayman IPA\nname=Deep Ellum IPA\nname=Des Moines IPA\nname=Descender IPA\nname=Desert Magic IPA\nname=Desolation IPA\nname=Disco Superfly\nname=Divided Sky";
        CalciteAssert.that().with(newConnectionFactory()).query("select name from redisearch.beers where style = 'American IPA' order by name").returnsOrdered(expected);
    }

    @Test
    void testFilterSort() {
        final String sql = "select * from redisearch.beers where style = 'American IPA' and abv >= .05 order by abv";
        final String explain = "PLAN=RediSearchToEnumerableConverter\n  RediSearchSort(sort0=[$2], dir0=[ASC])\n    RediSearchFilter(condition=[AND(=(CAST($1):VARCHAR, 'American IPA'), >=($2, 0.05:DECIMAL(2, 2)))])\n      RediSearchTableScan(table=[[redisearch, beers]])\n\n";
        final String returns = "name=Citrafest; style=American IPA; abv=0.05; brewery_id=27\nname=Grand Circus IPA; style=American IPA; abv=0.05; brewery_id=72\nname=Lasso; style=American IPA; abv=0.05; brewery_id=6\nname=Grapefruit IPA; style=American IPA; abv=0.05; brewery_id=13\nname=Lil SIPA; style=American IPA; abv=0.05; brewery_id=321\nname=Jah Mon; style=American IPA; abv=0.05; brewery_id=43\nname=Dayman IPA; style=American IPA; abv=0.05; brewery_id=43\nname=TailGate IPA; style=American IPA; abv=0.05; brewery_id=449\nname=TailGate IPA; style=American IPA; abv=0.05; brewery_id=449\nname=South Bay Session IPA; style=American IPA; abv=0.05; brewery_id=33\nname=Hop Session; style=American IPA; abv=0.05; brewery_id=47\nname=Quaff India Style Session Ale; style=American IPA; abv=0.051; brewery_id=201\nname=Firewater India Pale Ale; style=American IPA; abv=0.052000000000000005; brewery_id=331\nname=Firewater India Pale Ale; style=American IPA; abv=0.052000000000000005; brewery_id=331\nname=Self Starter; style=American IPA; abv=0.052000000000000005; brewery_id=94\nname=Fisherman's IPA; style=American IPA; abv=0.055; brewery_id=230\nname=Mosaic Single Hop IPA; style=American IPA; abv=0.055; brewery_id=41\nname=Manayunk IPA; style=American IPA; abv=0.055; brewery_id=356\nname=Pump House IPA; style=American IPA; abv=0.055; brewery_id=68\nname=Texas Pale Ale (TPA); style=American IPA; abv=0.055; brewery_id=257\nname=Nice Rack IPA; style=American IPA; abv=0.055; brewery_id=436\nname=Ghost Ship White IPA; style=American IPA; abv=0.055999999999999994; brewery_id=192\nname=Duluchan India Pale Ale; style=American IPA; abv=0.055999999999999994; brewery_id=345\nname=Wachusett IPA; style=American IPA; abv=0.055999999999999994; brewery_id=295\nname=IPA #11; style=American IPA; abv=0.057; brewery_id=121\nname=Tumbleweed IPA; style=American IPA; abv=0.057; brewery_id=537\nname=Sockeye Red IPA; style=American IPA; abv=0.057; brewery_id=223\nname=Triangle India Pale Ale; style=American IPA; abv=0.057; brewery_id=524\nname=Mango Ginger; style=American IPA; abv=0.057999999999999996; brewery_id=167\nname=Hop Farm IPA; style=American IPA; abv=0.057999999999999996; brewery_id=297\nname=Alloy; style=American IPA; abv=0.057999999999999996; brewery_id=17\nname=Charlie in the Rye; style=American IPA; abv=0.057999999999999996; brewery_id=351\nname=Train Hopper; style=American IPA; abv=0.057999999999999996; brewery_id=14\nname=Rude Parrot IPA; style=American IPA; abv=0.059000000000000004; brewery_id=481\nname=Liberty Ale; style=American IPA; abv=0.059000000000000004; brewery_id=35\nname=Soul Doubt; style=American IPA; abv=0.059000000000000004; brewery_id=66\nname=Point the Way IPA; style=American IPA; abv=0.059000000000000004; brewery_id=240\nname=Point the Way IPA; style=American IPA; abv=0.059000000000000004; brewery_id=240\nname=Point the Way IPA (2012); style=American IPA; abv=0.059000000000000004; brewery_id=240\nname=Goose Island India Pale Ale; style=American IPA; abv=0.059000000000000004; brewery_id=196\nname=Harpoon IPA; style=American IPA; abv=0.059000000000000004; brewery_id=234\nname=Harpoon IPA (2012); style=American IPA; abv=0.059000000000000004; brewery_id=234\nname=Harpoon IPA (2010); style=American IPA; abv=0.059000000000000004; brewery_id=234\nname=Troopers Alley IPA; style=American IPA; abv=0.059000000000000004; brewery_id=344\nname=Pile of Face; style=American IPA; abv=0.06; brewery_id=1\nname=Charlie's Rye IPA; style=American IPA; abv=0.06; brewery_id=146\nname=Shiva IPA; style=American IPA; abv=0.06; brewery_id=528\nname=Barrio Blanco; style=American IPA; abv=0.06; brewery_id=251\nname=House Brand IPA; style=American IPA; abv=0.06; brewery_id=519\nname=Linnaeus Mango IPA; style=American IPA; abv=0.06; brewery_id=10\nname=11th Hour IPA; style=American IPA; abv=0.06; brewery_id=212\nname=Half Cycle IPA; style=American IPA; abv=0.06; brewery_id=16\nname=Backyahd IPA; style=American IPA; abv=0.06; brewery_id=279\nname=The 26th; style=American IPA; abv=0.06; brewery_id=22\nname=Good People IPA; style=American IPA; abv=0.06; brewery_id=478\nname=JP's Ould Sod Irish Red IPA; style=American IPA; abv=0.06; brewery_id=32\nname=KelSo India Pale Ale; style=American IPA; abv=0.06; brewery_id=342\nname=King Street IPA; style=American IPA; abv=0.06; brewery_id=102\nname=Saranac White IPA; style=American IPA; abv=0.06; brewery_id=299\nname=Homefront IPA; style=American IPA; abv=0.06; brewery_id=163\nname=40 Mile IPA; style=American IPA; abv=0.06; brewery_id=273\nname=The Green Room; style=American IPA; abv=0.06; brewery_id=126\nname=Dragonfly IPA; style=American IPA; abv=0.06; brewery_id=202\nname=Green Monsta IPA; style=American IPA; abv=0.06; brewery_id=295\nname=Green Monsta IPA; style=American IPA; abv=0.06; brewery_id=295\nname=Super G IPA; style=American IPA; abv=0.06; brewery_id=396\nname=Fairweather IPA; style=American IPA; abv=0.061; brewery_id=493\nname=Caldera IPA (2009); style=American IPA; abv=0.061; brewery_id=155\nname=Caldera IPA (2007); style=American IPA; abv=0.061; brewery_id=155\nname=Caldera IPA; style=American IPA; abv=0.061; brewery_id=155\nname=Category 3 IPA; style=American IPA; abv=0.061; brewery_id=340\nname=Lumberyard IPA; style=American IPA; abv=0.061; brewery_id=158\nname=5 Day IPA; style=American IPA; abv=0.061; brewery_id=442\nname=Camelback; style=American IPA; abv=0.061; brewery_id=157\nname=Damnesia; style=American IPA; abv=0.062; brewery_id=401\nname=Desolation IPA; style=American IPA; abv=0.062; brewery_id=401\nname=Fire Eagle IPA; style=American IPA; abv=0.062; brewery_id=413\nname=Bent Hop Golden IPA; style=American IPA; abv=0.062; brewery_id=75\nname=Ryecoe; style=American IPA; abv=0.062; brewery_id=8\nname=Big Sky IPA; style=American IPA; abv=0.062; brewery_id=336\nname=Big Sky IPA (2012); style=American IPA; abv=0.062; brewery_id=336\nname=Heavy Lifting; style=American IPA; abv=0.062; brewery_id=31\nname=Lucky U IPA; style=American IPA; abv=0.062; brewery_id=391\nname=Mutiny IPA; style=American IPA; abv=0.062; brewery_id=192\nname=Operation Homefront; style=American IPA; abv=0.062; brewery_id=141\nname=Escape to Colorado; style=American IPA; abv=0.062; brewery_id=81\nname=Country Boy IPA; style=American IPA; abv=0.062; brewery_id=170\nname=The Optimist; style=American IPA; abv=0.062; brewery_id=206\nname=Great Crescent IPA; style=American IPA; abv=0.062; brewery_id=165\nname=Great Crescent IPA (2011); style=American IPA; abv=0.062; brewery_id=165\nname=Face Plant IPA; style=American IPA; abv=0.062; brewery_id=430\nname=Marble India Pale Ale; style=American IPA; abv=0.062; brewery_id=443\nname=Big Swell IPA; style=American IPA; abv=0.062; brewery_id=375\nname=Sea Hag IPA; style=American IPA; abv=0.062; brewery_id=410\nname=Sea Hag IPA (Current); style=American IPA; abv=0.062; brewery_id=410\nname=Outlaw IPA; style=American IPA; abv=0.062; brewery_id=307\nname=Amazon Princess IPA; style=American IPA; abv=0.062; brewery_id=205\nname=Gangway IPA; style=American IPA; abv=0.062; brewery_id=475\nname=Old Wylie's IPA; style=American IPA; abv=0.062; brewery_id=43\nname=360° India Pale Ale; style=American IPA; abv=0.062; brewery_id=371";
        final String query = "(@style:{American IPA} @abv:[0.05 inf])";
        calciteAssert().query(sql).returnsOrdered(returns).queryContains(RediSearchChecker.rediSearchChecker(query)).explainContains(explain);
    }

    @Test
    void testBeers() {
        calciteAssert().query("select name, style from redisearch.beers").returnsCount(RediSearchTable.DEFAULT_LIMIT);
    }

    @Test
    void testFilterReversed() {
        calciteAssert().query("select name, abv from redisearch.beers where .1 < abv order by name").limit(2).returnsUnordered("name=4Beans; abv=0.1\nname=Csar; abv=0.12");
        calciteAssert().query("select name, abv from redisearch.beers where abv > .1 order by name").limit(2).returnsUnordered("name=4Beans; abv=0.1\nname=Csar; abv=0.12");
    }


}
