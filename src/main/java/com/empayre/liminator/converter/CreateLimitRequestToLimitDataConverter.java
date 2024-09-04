package com.empayre.liminator.converter;

import com.empayre.liminator.domain.tables.pojos.LimitData;
import dev.vality.liminator.CreateLimitRequest;
import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.LocalDateTime;

@Component
public class CreateLimitRequestToLimitDataConverter implements Converter<CreateLimitRequest, LimitData> {

    @Override
    public LimitData convert(CreateLimitRequest request) {
        LimitData data = new LimitData();
        data.setName(request.getLimitName());
        data.setCreatedAt(LocalDate.now());
        data.setWtime(LocalDateTime.now());
        return data;
    }
}
