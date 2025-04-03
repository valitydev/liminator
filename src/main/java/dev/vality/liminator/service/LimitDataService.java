package dev.vality.liminator.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.vality.liminator.InvalidRequest;
import dev.vality.liminator.LimitChange;
import dev.vality.liminator.LimitNotFound;
import dev.vality.liminator.LimitRequest;
import dev.vality.liminator.dao.LimitContextDao;
import dev.vality.liminator.dao.LimitDataDao;
import dev.vality.liminator.domain.tables.pojos.LimitContext;
import dev.vality.liminator.domain.tables.pojos.LimitData;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static dev.vality.liminator.domain.enums.OperationState.ROLLBACK;

@Slf4j
@Service
@RequiredArgsConstructor
public class LimitDataService {

    private static final String EMPTY_JSON = "{}";

    private final ObjectMapper mapper;
    private final Converter<LimitChange, LimitData> limitChangeToLimitDataConverter;
    private final LimitDataDao limitDataDao;
    private final LimitContextDao limitContextDao;

    public List<LimitData> get(LimitRequest request, String source) throws TException {
        List<String> limitNames = request.getLimitChanges().stream()
                .map(LimitChange::getLimitName)
                .toList();
        List<LimitData> limitData = limitDataDao.get(limitNames);
        if (CollectionUtils.isEmpty(limitData)) {
            log.error("[{}] Limits not found: {}", source, limitNames);
            if (Objects.equals(source, ROLLBACK.getLiteral())) {
                throw new InvalidRequest();
            }
            throw new LimitNotFound();
        }
        if (limitData.size() != limitNames.size()) {
            log.error("[{}] Received limit ({}) size is not equal to expected ({}). " +
                    "Probably one of limits doesn't exist", source, limitData.size(), limitNames.size());
            throw new LimitNotFound();
        }
        return limitData;
    }

    public LimitData get(String limitName) throws TException {
        log.debug("Try to get limit for limitName: {}", limitName);
        return limitDataDao.get(limitName);
    }

    @Transactional
    public Long save(LimitChange change) {
        log.info("Save limit: {}", change.getLimitName());
        LimitData limitData = limitChangeToLimitDataConverter.convert(change);
        Long limitId = limitDataDao.save(limitData);
        if (change.context != null) {
            limitContextDao.save(convertToLimitContext(limitId, change));
        }

        return limitId;
    }

    private LimitContext convertToLimitContext(Long limitId, LimitChange change) {
        LimitContext context = new LimitContext();
        context.setLimitDataId(limitId);
        context.setContext(getContextString(change.getContext()));
        context.setWtime(LocalDateTime.now());
        return context;
    }

    private String getContextString(Map<String, String> contextMap) {
        try {
            return mapper.writeValueAsString(contextMap);
        } catch (JsonProcessingException e) {
            log.error("ContextJSON processing exception", e);
            return EMPTY_JSON;
        }
    }
}
