package com.empayre.liminator.service;

import com.empayre.liminator.dao.LimitDataDao;
import com.empayre.liminator.domain.tables.pojos.LimitData;
import com.empayre.liminator.util.LimitDataUtils;
import dev.vality.liminator.LimitNotFound;
import dev.vality.liminator.LimitRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class LimitsGettingService {

    private final LimitDataDao limitDataDao;

    public List<LimitData> get(List<LimitRequest> requestList, String source) throws TException {
        List<String> limitNames = LimitDataUtils.getLimitNames(requestList);
        List<LimitData> limitData = limitDataDao.get(limitNames);
        if (CollectionUtils.isEmpty(limitData)) {
            log.error("[{}] Limits not found: {}", source, limitNames);
            throw new LimitNotFound();
        }
        if (limitData.size() != limitNames.size()) {
            log.error("[{}] Received limit ({}) size is not equal to expected ({}). " +
                    "Probably one of limits doesn't exist", source, limitData.size(), limitNames.size());
            throw new LimitNotFound();
        }
        return limitData;
    }
}
