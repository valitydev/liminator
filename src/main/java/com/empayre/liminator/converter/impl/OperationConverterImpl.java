package com.empayre.liminator.converter.impl;

import com.empayre.liminator.converter.OperationConverter;
import com.empayre.liminator.domain.enums.OperationState;
import com.empayre.liminator.domain.tables.pojos.Operation;
import dev.vality.liminator.LimitRequest;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Component
public class OperationConverterImpl implements OperationConverter {

    @Override
    public Operation convert(LimitRequest request, Long limitId) {
        Operation operation = new Operation();
        operation.setLimitId(limitId);
        operation.setOperationId(request.getOperationId());
        operation.setOperationValue(request.getValue());
        operation.setCreatedAt(LocalDateTime.now());
        operation.setState(OperationState.HOLD);
        return operation;
    }
}
