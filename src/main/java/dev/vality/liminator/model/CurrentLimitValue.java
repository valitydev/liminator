package dev.vality.liminator.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class CurrentLimitValue {

    private String limitId;
    private String limitName;
    private Long holdValue;
    private Long commitValue;
}
