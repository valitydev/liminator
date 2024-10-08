package com.empayre.liminator.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class LimitValue {

    private String limitId;
    private String limitName;
    private Long commitValue;
    private Long holdValue;
    private Long rollbackValue;
}
