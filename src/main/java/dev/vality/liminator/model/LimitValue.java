package dev.vality.liminator.model;

import dev.vality.liminator.domain.enums.OperationState;
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
