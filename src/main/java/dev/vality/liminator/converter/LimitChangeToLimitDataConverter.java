package dev.vality.liminator.converter;

import dev.vality.liminator.domain.tables.pojos.LimitData;
import dev.vality.liminator.LimitChange;
import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.LocalDateTime;

@Component
public class LimitChangeToLimitDataConverter implements Converter<LimitChange, LimitData> {

    @Override
    public LimitData convert(LimitChange change) {
        LimitData data = new LimitData();
        data.setName(change.getLimitName());
        data.setLimitId(change.getLimitId());
        data.setCreatedAt(LocalDate.now());
        data.setWtime(LocalDateTime.now());
        return data;
    }
}
