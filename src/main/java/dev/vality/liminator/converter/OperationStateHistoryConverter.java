package dev.vality.liminator.converter;

import dev.vality.liminator.domain.enums.OperationState;
import dev.vality.liminator.domain.tables.pojos.OperationStateHistory;
import dev.vality.liminator.LimitRequest;

import java.util.List;
import java.util.Map;

public interface OperationStateHistoryConverter {

    List<OperationStateHistory> convert(LimitRequest request, OperationState state, Map<String, Long> limitNamesMap);
}
