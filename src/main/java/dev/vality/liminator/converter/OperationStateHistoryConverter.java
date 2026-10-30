package dev.vality.liminator.converter;

import dev.vality.liminator.LimitChange;
import dev.vality.liminator.domain.enums.OperationState;
import dev.vality.liminator.domain.tables.pojos.OperationStateHistory;

import java.util.List;
import java.util.Map;

public interface OperationStateHistoryConverter {

    List<OperationStateHistory> convert(String operationId,
                                        List<LimitChange> limitChanges,
                                        OperationState state,
                                        Map<String, Long> limitNamesMap);
}
