package com.empayre.liminator.converter;

import com.empayre.liminator.domain.enums.OperationState;
import com.empayre.liminator.domain.tables.pojos.OperationStateHistory;
import dev.vality.liminator.LimitRequest;

import java.util.List;
import java.util.Map;

public interface OperationStateHistoryConverter {

    List<OperationStateHistory> convert(LimitRequest request, OperationState state, Map<String, Long> limitNamesMap);
}
