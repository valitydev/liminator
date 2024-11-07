package dev.vality.liminator.converter.impl;

import dev.vality.liminator.converter.OperationConverter;
import dev.vality.liminator.domain.enums.OperationState;
import dev.vality.liminator.domain.tables.pojos.Operation;
import dev.vality.liminator.LimitRequest;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Component
public class OperationConverterImpl implements OperationConverter {

    @Override
    public Operation convert(LimitRequest request, Long limitId, Long limitValue, LocalDateTime createdAt) {
        Operation operation = new Operation();
        operation.setLimitId(limitId);
        operation.setOperationId(request.getOperationId());
        operation.setOperationValue(limitValue);
        operation.setCreatedAt(createdAt);
        operation.setState(OperationState.HOLD);
        return operation;
    }
}
