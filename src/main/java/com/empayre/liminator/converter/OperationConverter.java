package com.empayre.liminator.converter;

import com.empayre.liminator.domain.tables.pojos.Operation;
import dev.vality.liminator.LimitRequest;

import java.time.LocalDateTime;

public interface OperationConverter {

    Operation convert(LimitRequest request, Long limitId, Long limitValue, LocalDateTime createdAt);
}
