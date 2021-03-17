package com.redislabs.jdbc;

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
        CalciteAssert.that().with(newConnectionFactory()).query("select * from redisearch.beers").returns("name=Rail Yard Ale (2009); style=American Amber / Red Ale; abv=0.052000000000000005; brewery_id=424\nname=Silverback Pale Ale; style=American Pale Ale (APA); abv=0.055; brewery_id=424\nname=B3K Black Lager; style=Schwarzbier; abv=0.055; brewery_id=424\nname=Rail Yard Ale; style=American Amber / Red Ale; abv=0.052000000000000005; brewery_id=424\nname=Belgorado; style=Belgian IPA; abv=0.067; brewery_id=424\nname=Rocky Mountain Oyster Stout; style=American Stout; abv=0.075; brewery_id=424\nname=Wynkoop Pumpkin Ale; style=Pumpkin Ale; abv=0.055; brewery_id=424\nname=Colorojo Imperial Red Ale; style=American Strong Ale; abv=0.08199999999999999; brewery_id=424\nname=Worthy Pale; style=American Pale Ale (APA); abv=0.06; brewery_id=199\nname=Worthy IPA (2013); style=American IPA; abv=0.069; brewery_id=199\n");
    }

    @Test
    void filter() {
        CalciteAssert.that().with(newConnectionFactory()).query("select * from redisearch.beers where name = 'Sinister'").returnsCount(4);
        CalciteAssert.that().with(newConnectionFactory()).query("select * from redisearch.beers where name in ('SINISTER', 'FOREMAN')").returnsCount(DEFAULT_LIMIT_NUM);
        CalciteAssert.that().with(newConnectionFactory()).query("select * from redisearch.beers limit 0").returnsCount(0);
    }

    @Test
    void testSort() {
        final String explain = "PLAN=RediSearchToEnumerableConverter\n  RediSearchSort(sort0=[$2], dir0=[ASC])\n    RediSearchTableScan(table=[[redisearch, beers]])\n\n";
        calciteAssert().query("select * from redisearch.beers order by abv").returnsCount(DEFAULT_LIMIT_NUM).returns(sortedResultSetChecker("abv", RelFieldCollation.Direction.ASCENDING)).explainContains(explain);
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
        CalciteAssert.that().with(newConnectionFactory()).query("select * from redisearch.beers order by name").returnsCount(DEFAULT_LIMIT_NUM);
        CalciteAssert.that().with(newConnectionFactory()).query("select style from redisearch.beers order by name").returnsCount(DEFAULT_LIMIT_NUM);
        CalciteAssert.that().with(newConnectionFactory()).query("select name from redisearch.beers where style = 'American IPA' order by name").returnsOrdered("name=#002 American I.P.A.\nname=#004 Session I.P.A.\nname=113 IPA\nname=11th Hour IPA\nname=1916 Shore Shiver\nname=2014 IPA Cicada Series\nname=2020 IPA\nname=21st Amendment IPA (2006)\nname=3-Way IPA (2013)\nname=360° India Pale Ale");
    }

    @Test
    void testFilterSort() {
        final String sql = "select * from redisearch.beers where style = 'American IPA' and abv >= .05 order by abv";
        final String explain = "PLAN=RediSearchToEnumerableConverter\n  RediSearchSort(sort0=[$2], dir0=[ASC])\n    RediSearchFilter(condition=[AND(=(CAST($1):VARCHAR, 'American IPA'), >=($2, 0.05:DECIMAL(2, 2)))])\n      RediSearchTableScan(table=[[redisearch, beers]])\n\n";
        final String returns = "name=Citrafest; style=American IPA; abv=0.05; brewery_id=27\nname=Grand Circus IPA; style=American IPA; abv=0.05; brewery_id=72\nname=Lasso; style=American IPA; abv=0.05; brewery_id=6\nname=Grapefruit IPA; style=American IPA; abv=0.05; brewery_id=13\nname=Lil SIPA; style=American IPA; abv=0.05; brewery_id=321\nname=Jah Mon; style=American IPA; abv=0.05; brewery_id=43\nname=Dayman IPA; style=American IPA; abv=0.05; brewery_id=43\nname=TailGate IPA; style=American IPA; abv=0.05; brewery_id=449\nname=TailGate IPA; style=American IPA; abv=0.05; brewery_id=449\nname=South Bay Session IPA; style=American IPA; abv=0.05; brewery_id=33";
        final String query = "(@style:{American IPA} @abv:[0.05 inf])";
        calciteAssert().query(sql).returnsOrdered(returns).queryContains(RediSearchChecker.rediSearchChecker(query)).explainContains(explain);
    }

    @Test
    void testBeers() {
        calciteAssert().query("select name, style from redisearch.beers").returnsCount(DEFAULT_LIMIT_NUM);
    }

    @Test
    void testFilterReversed() {
        calciteAssert().query("select name, abv from redisearch.beers where .1 < abv order by name").limit(2).returnsUnordered("name=4Beans; abv=0.1\nname=Csar; abv=0.12");
        calciteAssert().query("select name, abv from redisearch.beers where abv > .1 order by name").limit(2).returnsUnordered("name=4Beans; abv=0.1\nname=Csar; abv=0.12");
    }


}
