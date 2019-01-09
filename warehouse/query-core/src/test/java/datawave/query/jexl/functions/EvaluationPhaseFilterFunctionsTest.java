package datawave.query.jexl.functions;

import com.google.common.collect.Lists;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.Type;
import datawave.query.attributes.TypeAttribute;
import datawave.query.attributes.ValueTuple;
import org.apache.accumulo.core.data.Key;
import org.junit.Assert;
import org.junit.Test;

public class EvaluationPhaseFilterFunctionsTest {
    
    private ValueTuple makeValueTuple(String csv) {
        String[] tokens = csv.split(",");
        String field = tokens[0];
        Type<String> type = new LcNoDiacriticsType(tokens[1]);
        TypeAttribute<String> typeAttribute = new TypeAttribute<>(type, new Key(), true);
        String normalized = tokens[2];
        return new ValueTuple(field, type, normalized, typeAttribute);
    }
    
    @Test
    public void testIncludeRegex() {
        // @formatter:off
        // will return the first (thus only one) match
        Assert.assertTrue(EvaluationPhaseFilterFunctions.includeRegex(
                Lists.newArrayList(
                        makeValueTuple("FOO.1,BAR,bar"),
                        makeValueTuple("FOO.2,BAR,bar"),
                        makeValueTuple("FOO.3,BAZ,baz")),
                "ba.*").size() == 1);
        // will return the set of all unique matches
        Assert.assertTrue(EvaluationPhaseFilterFunctions.getAllMatches(
                Lists.newArrayList(
                        makeValueTuple("FOO.1,BAR,bar"),
                        makeValueTuple("FOO.2,BAZ,baz"),
                        makeValueTuple("FOO.3,BAR,bar")),
                "ba.*").size() == 3);
        // @formatter:on
    }
    
    @Test
    public void testMatchesAtLeastCountOf() {
        // @formatter:off
        Assert.assertTrue(EvaluationPhaseFilterFunctions.matchesAtLeastCountOf(
                3,
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"),
                        makeValueTuple("STOOGE.2,LARRY,larry"),
                        makeValueTuple("STOOGE.3,JOE,joe")),
                "MOE", "LARRY", "JOE", "SHEMP", "CURLEY JOE"
        ).size() == 3);

        Assert.assertTrue(EvaluationPhaseFilterFunctions.matchesAtLeastCountOf(
                3,
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"),
                        makeValueTuple("STOOGE.2,LARRY,larry"),
                        makeValueTuple("STOOGE.3,GROUCHO,groucho")),
                "MOE", "LARRY", "JOE", "SHEMP", "CURLEY JOE"
        ).size() == 0);
        // @formatter:on
    }
}
