package dev.gamov.streams.flink;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.HashSet;

import dev.gamov.streams.EnrichedClick;

/**
 * AggregateFunction to count unique users per category
 */
public class UniqueUsersAggregateFunction implements
                                          AggregateFunction<EnrichedClick, Tuple2<String, HashSet<String>>, Tuple2<String, String>> {

  @Override
  public Tuple2<String, HashSet<String>> createAccumulator() {
    return new Tuple2<>("", new HashSet<>());
  }

  @Override
  public Tuple2<String, HashSet<String>> add(EnrichedClick value, Tuple2<String, HashSet<String>> accumulator) {
    // Store the category in the accumulator
    if (accumulator.f0.isEmpty()) {
      accumulator.f0 = value.getCategory();
    }
    accumulator.f1.add(value.getUserId());
    return accumulator;
  }

  @Override
  public Tuple2<String, String> getResult(Tuple2<String, HashSet<String>> accumulator) {
    String category = accumulator.f0;
    String result = "Count: " + accumulator.f1.size();
    return new Tuple2<>(category, result);
  }

  @Override
  public Tuple2<String, HashSet<String>> merge(Tuple2<String, HashSet<String>> a, Tuple2<String, HashSet<String>> b) {
    // Use the category from the first accumulator if it's not empty
    String category = a.f0.isEmpty() ? b.f0 : a.f0;
    HashSet<String> mergedUsers = new HashSet<>(a.f1);
    mergedUsers.addAll(b.f1);
    return new Tuple2<>(category, mergedUsers);
  }
}
