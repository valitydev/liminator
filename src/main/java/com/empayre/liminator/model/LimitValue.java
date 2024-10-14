package com.empayre.liminator.model;

import com.empayre.liminator.domain.enums.OperationState;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class LimitValue {

    private String limitId;
    private String limitName;
    private OperationState state;
    private Long operationValue;
}
