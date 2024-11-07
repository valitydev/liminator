package dev.vality.liminator.dao.mapper;

import dev.vality.liminator.domain.enums.OperationState;
import dev.vality.liminator.model.LimitValue;
import org.jooq.Record;
import org.jooq.RecordMapper;
import org.springframework.stereotype.Component;

import static dev.vality.liminator.domain.Tables.OPERATION_STATE_HISTORY;
import static dev.vality.liminator.domain.Tables.LIMIT_DATA;

@Component
public class LimitValueMapper implements RecordMapper<Record, LimitValue> {

    @Override
    public LimitValue map(Record opsDataRecord) {
        String limitId = opsDataRecord.get(LIMIT_DATA.LIMIT_ID);
        String limitName = opsDataRecord.get(LIMIT_DATA.NAME);
        Long operationValue = opsDataRecord.get(OPERATION_STATE_HISTORY.OPERATION_VALUE);
        OperationState state = OperationState.valueOf(opsDataRecord.get("state", String.class));
        return new LimitValue(limitId, limitName, state, operationValue);
    }
}
