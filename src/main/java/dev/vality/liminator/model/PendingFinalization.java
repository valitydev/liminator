package dev.vality.liminator.model;

import dev.vality.liminator.LimitChange;

import java.util.List;
import java.util.Map;

public record PendingFinalization(List<LimitChange> changes, Map<String, Long> limitNamesMap) {
}
