package com.empayre.liminator.converter.impl;

import com.empayre.liminator.converter.OperationStateHistoryConverter;
import com.empayre.liminator.domain.enums.OperationState;
import com.empayre.liminator.domain.tables.pojos.OperationStateHistory;
import dev.vality.liminator.LimitRequest;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class OperationStateHistoryConverterImpl implements OperationStateHistoryConverter {

    @Override
    public List<OperationStateHistory> convert(LimitRequest request, OperationState state) {
        return request.getLimitNames().stream()
                .map(limitName -> {
                    OperationStateHistory history = new OperationStateHistory();
                    history.setOperationId(request.getOperationId());
                    history.setLimitName(limitName);
                    history.setOperationValue(request.getValue());
                    history.setState(state);
                    return history;
                })
                .toList();
    }
}
