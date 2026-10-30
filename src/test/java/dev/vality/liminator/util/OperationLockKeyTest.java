package dev.vality.liminator.util;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static dev.vality.liminator.util.OperationLockKey.fromOperationId;
import static org.junit.jupiter.api.Assertions.assertEquals;

class OperationLockKeyTest {

    @ParameterizedTest
    @CsvSource({
            "operation-1, 4492479431599175482",
            "операция-ёж, 335847432768951267",
            "'', -6462532317919139451",
            "Aa, -2860361877746744868",
            "BB, 55278138067037764"
    })
    void usesStableNamespacedUtf8Digest(String operationId, long expected) {
        assertEquals(expected, fromOperationId(operationId));
        assertEquals(expected, fromOperationId(operationId));
    }
}
