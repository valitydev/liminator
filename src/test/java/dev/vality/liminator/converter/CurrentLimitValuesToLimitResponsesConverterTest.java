package dev.vality.liminator.converter;

import dev.vality.liminator.LimitResponse;
import dev.vality.liminator.model.CurrentLimitValue;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CurrentLimitValuesToLimitResponsesConverterTest {

    private final CurrentLimitValuesToLimitResponsesConverter converter = new CurrentLimitValuesToLimitResponsesConverter();

    @Test
    void shouldReturnZeroTotalWhenCommitWasAppliedWithZeroValue() {
        CurrentLimitValue value = new CurrentLimitValue("limit-id", "limit-name", 500L, 0L, 1);

        LimitResponse response = converter.convert(List.of(value)).get(0);

        assertEquals(0L, response.getTotalValue());
        assertEquals(0L, response.getCommitValue());
    }

    @Test
    void shouldReturnHoldPlusCommitForRegularCase() {
        CurrentLimitValue value = new CurrentLimitValue("limit-id", "limit-name", 500L, 300L, 1);

        LimitResponse response = converter.convert(List.of(value)).get(0);

        assertEquals(800L, response.getTotalValue());
        assertEquals(300L, response.getCommitValue());
    }
}
