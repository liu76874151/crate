package org.cratedb.service;

import org.cratedb.action.sql.SQLAction;
import org.cratedb.action.sql.SQLRequest;
import org.cratedb.action.sql.analyzer.AnalyzerService;
import org.cratedb.sql.parser.StandardException;
import org.elasticsearch.ElasticsearchTestCase;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.internal.InternalNode;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.hamcrest.Matchers.*;

public class AnalyzerServiceTest extends ElasticsearchTestCase {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    private static final Settings defaultSettings = ImmutableSettings
            .settingsBuilder()
            .put("cluster.name", "test-cluster-" + NetworkUtils.getLocalAddress().getHostName() + "CHILD_VM=[" + CHILD_VM_ID +"]")
            .build();
    private static InternalNode node;
    private static AnalyzerService analyzerService;

    @BeforeClass
    public static void setupClass() {
        synchronized (AnalyzerServiceTest.class) {
            String settingsSource = InformationSchemaQueryTest.class.getName().replace('.', '/') + ".yml";
            Settings finalSettings = settingsBuilder()
                    .loadFromClasspath(settingsSource)
                    .put(defaultSettings)
                    .put("name", "node1")
                    .put("discovery.id.seed", randomLong())
                    .build();

            if (finalSettings.get("gateway.type") == null) {
                // default to non gateway
                finalSettings = settingsBuilder().put(finalSettings).put("gateway.type", "none").build();
            }
            if (finalSettings.get("cluster.routing.schedule") != null) {
                // decrease the routing schedule so new nodes will be added quickly
                finalSettings = settingsBuilder().put(finalSettings).put("cluster.routing.schedule", "50ms").build();
            }

            node = (InternalNode)nodeBuilder().settings(finalSettings).build();
            node.start();
            analyzerService = node.injector().getInstance(AnalyzerService.class);
        }
    }

    @AfterClass
    public static void tearDownClass() {
        synchronized (AnalyzerServiceTest.class) {
            analyzerService = null;
            node.close();
            node = null;
        }
    }

    @Test
    public void resolveSimpleAnalyzerSettings() throws StandardException {
        node.client().execute(SQLAction.INSTANCE, new SQLRequest("CREATE ANALYZER a1 WITH" +
                "(tokenizer lowercase)"))
                .actionGet();
        Settings fullAnalyzerSettings = analyzerService.resolveFullCustomAnalyzerSettings("a1");
        assertThat(fullAnalyzerSettings.getAsMap().size(), is(2));
        assertThat(
                fullAnalyzerSettings.getAsMap(),
                hasEntry("index.analysis.analyzer.a1.type", "custom")
        );
        assertThat(
                fullAnalyzerSettings.getAsMap(),
                hasEntry("index.analysis.analyzer.a1.tokenizer", "lowercase")
        );
    }

    @Test
    public void resolveAnalyzerWithCustomTokenizer() throws StandardException {
        node.client().execute(SQLAction.INSTANCE, new SQLRequest("CREATE ANALYZER a2 WITH" +
                "(" +
                "   tokenizer tok2 with (" +
                "       type='ngram'," +
                "       \"min_ngram\"=2," +
                "       \"token_chars\"=['letter', 'digits']" +
                "   )" +
                ")"))
                .actionGet();
        Settings fullAnalyzerSettings = analyzerService.resolveFullCustomAnalyzerSettings("a2");
        assertThat(
                fullAnalyzerSettings.getAsMap(),
                hasEntry("index.analysis.analyzer.a2.type", "custom")
        );
        assertThat(
                fullAnalyzerSettings.getAsMap(),
                hasEntry("index.analysis.analyzer.a2.tokenizer", "a2_tok2")
        );
        assertThat(
                fullAnalyzerSettings.getAsMap(),
                allOf(
                        hasEntry("index.analysis.tokenizer.a2_tok2.type", "ngram"),
                        hasEntry("index.analysis.tokenizer.a2_tok2.min_ngram", "2"),
                        hasEntry("index.analysis.tokenizer.a2_tok2.token_chars.0", "letter"),
                        hasEntry("index.analysis.tokenizer.a2_tok2.token_chars.1", "digits")
                )
        );
    }

    @Test
    public void resolveAnalyzerWithCharFilters() throws StandardException {
        node.client().execute(SQLAction.INSTANCE, new SQLRequest("CREATE ANALYZER a3 WITH" +
                "(" +
                "   tokenizer lowercase," +
                "   char_filters WITH (" +
                "       \"html_strip\"," +
                "       mymapping WITH (" +
                "           type='mapping'," +
                "           mappings=['ph=>f', 'ß=>ss', 'ö=>oe']" +
                "       )" +
                "   )" +
                ")"))
                .actionGet();
        Settings fullAnalyzerSettings = analyzerService.resolveFullCustomAnalyzerSettings("a3");
        assertThat(
                fullAnalyzerSettings.getAsMap(),
                hasEntry("index.analysis.analyzer.a3.type", "custom")
        );
        assertThat(
                fullAnalyzerSettings.getAsMap(),
                hasEntry("index.analysis.analyzer.a3.tokenizer", "lowercase")
        );
        assertThat(
                fullAnalyzerSettings.getAsArray("index.analysis.analyzer.a3.char_filter"),
                arrayContainingInAnyOrder("html_strip", "a3_mymapping")
        );
        assertThat(
                fullAnalyzerSettings.getAsMap(),
                hasEntry("index.analysis.char_filter.a3_mymapping.type", "mapping")
        );
        assertThat(
                fullAnalyzerSettings.getAsArray("index.analysis.char_filter.a3_mymapping" +
                        ".mappings"),
                arrayContainingInAnyOrder("ph=>f", "ß=>ss", "ö=>oe")
        );
    }

    @Test
    public void resolveAnalyzerExtendingBuiltin() throws StandardException {
        node.client().execute(SQLAction.INSTANCE, new SQLRequest("CREATE ANALYZER a4 EXTENDS " +
                "german WITH (" +
                "   \"stop_words\"=['der', 'die', 'das']" +
                ")"))
                .actionGet();
        Settings fullAnalyzerSettings = analyzerService.resolveFullCustomAnalyzerSettings("a4");
        assertThat(
                fullAnalyzerSettings.getAsMap(),
                hasEntry("index.analysis.analyzer.a4.type", "german")
        );
        assertThat(
                fullAnalyzerSettings.getAsArray("index.analysis.analyzer.a4.stop_words"),
                arrayContainingInAnyOrder("der", "die", "das")
        );

        // extend analyzer who extends builtin analyzer (chain can be longer than 1)
        node.client().execute(SQLAction.INSTANCE, new SQLRequest("CREATE ANALYZER a4e EXTENDS " +
                "a4 WITH (" +
                "   \"stop_words\"=['der', 'die', 'das', 'wer', 'wie', 'was']" +
                ")"))
                .actionGet();
        fullAnalyzerSettings = analyzerService.resolveFullCustomAnalyzerSettings("a4e");
        assertThat(
                fullAnalyzerSettings.getAsMap(),
                hasEntry("index.analysis.analyzer.a4e.type", "german")
        );
        assertThat(
                fullAnalyzerSettings.getAsArray("index.analysis.analyzer.a4e.stop_words"),
                arrayContainingInAnyOrder("der", "die", "das", "wer", "wie", "was")
        );
    }

    @Test
    public void resolveAnalyzerExtendingCustom() throws StandardException {
        node.client().execute(SQLAction.INSTANCE, new SQLRequest("CREATE ANALYZER a5 WITH (" +
                "   tokenizer whitespace," +
                "   token_filters (" +
                "       lowercase," +
                "       germanstemmer WITH (" +
                "           type='stemmer'," +
                "           language='german'" +
                "       )" +
                "   )" +
                ")"))
                .actionGet();
        Settings fullAnalyzerSettings = analyzerService.resolveFullCustomAnalyzerSettings("a5");
        assertThat(
                fullAnalyzerSettings.getAsMap(),
                hasEntry("index.analysis.analyzer.a5.type", "custom")
        );
        assertThat(
                fullAnalyzerSettings.getAsMap(),
                hasEntry("index.analysis.analyzer.a5.tokenizer", "whitespace")
        );
        assertThat(
                fullAnalyzerSettings.getAsArray("index.analysis.analyzer.a5.filter"),
                arrayContainingInAnyOrder("lowercase", "a5_germanstemmer")
        );
        assertThat(
                fullAnalyzerSettings.getAsMap(),
                allOf(
                    hasEntry("index.analysis.filter.a5_germanstemmer.type", "stemmer"),
                    hasEntry("index.analysis.filter.a5_germanstemmer.language", "german")
                )
        );

        node.client().execute(SQLAction.INSTANCE, new SQLRequest("CREATE ANALYZER a5e EXTENDS a5" +
                " WITH (" +
                "   tokenizer letter," +
                "   char_filters WITH (" +
                "       \"html_strip\"," +
                "       mymapping WITH (" +
                "           type='mapping'," +
                "           mappings=['ph=>f', 'ß=>ss', 'ö=>oe']" +
                "       )" +
                "   )" +
                ")"))
                .actionGet();

        fullAnalyzerSettings = analyzerService.resolveFullCustomAnalyzerSettings("a5e");
        assertThat(
                fullAnalyzerSettings.getAsMap(),
                hasEntry("index.analysis.analyzer.a5e.type", "custom")
        );
        assertThat(
                fullAnalyzerSettings.getAsMap(),
                hasEntry("index.analysis.analyzer.a5e.tokenizer", "letter")
        );
        assertThat(
                fullAnalyzerSettings.getAsArray("index.analysis.analyzer.a5e.filter"),
                arrayContainingInAnyOrder("lowercase", "a5_germanstemmer")
        );
        assertThat(
                fullAnalyzerSettings.getAsMap(),
                allOf(
                        hasEntry("index.analysis.filter.a5_germanstemmer.type", "stemmer"),
                        hasEntry("index.analysis.filter.a5_germanstemmer.language", "german")
                )
        );
        assertThat(
                fullAnalyzerSettings.getAsArray("index.analysis.analyzer.a5e.char_filter"),
                arrayContainingInAnyOrder("html_strip", "a5e_mymapping")
        );
    }

}