package com.empayre.liminator.dao;

import com.empayre.liminator.domain.tables.pojos.LimitData;

import java.util.Collection;
import java.util.List;

public interface LimitDataDao extends CommonDao<LimitData> {

    LimitData get(String limitName);

    List<LimitData> get(Collection<String> limitNames);
}
