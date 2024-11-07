package dev.vality.liminator.converter.impl;

import dev.vality.liminator.converter.OperationStateHistoryConverter;
import dev.vality.liminator.domain.enums.OperationState;
import dev.vality.liminator.domain.tables.pojos.OperationStateHistory;
import dev.vality.liminator.LimitRequest;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

@Component
public class OperationStateHistoryConverterImpl implements OperationStateHistoryConverter {

    @Override
    public List<OperationStateHistory> convert(LimitRequest request,
                                               OperationState state,
                                               Map<String, Long> limitNamesMap) {
        var now = LocalDateTime.now();
        return request.getLimitChanges().stream()
                .map(change -> {
                    OperationStateHistory history = new OperationStateHistory();
                    history.setOperationId(request.getOperationId());
                    history.setLimitDataId(limitNamesMap.get(change.getLimitName()));
                    history.setOperationValue(change.getValue());
                    history.setState(state);
                    history.setCreatedAt(now);
                    return history;
                })
                .toList();
    }
}
