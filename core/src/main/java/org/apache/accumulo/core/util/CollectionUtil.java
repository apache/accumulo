package org.apache.accumulo.core.util;

import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class CollectionUtil {

 private CollectionUtil() {
     throw new UnsupportedOperationException();

 }

 public static <I,O,C extends Collection<O>> C transformElements(Collection<? extends I> collection,
                                                                 Function<I, O> transformer,
                                                                 Supplier<C> collectionFactory) {
     if (collection == null) {
         return null;
     }
     return collection.stream()
             .map(transformer)
             .collect(Collectors.toCollection(collectionFactory));
 }

 public static <I,O> List<O> toList(Collection<? extends I> collection, Function<I,O> transformer) {
     return transformElements(collection, transformer, ArrayList::new);
 }

 public static <I,O> Set<O> toSet(Collection<? extends I> collection, Function<I,O> transformer) {
     return transformElements(collection, transformer, HashSet::new);
 }

 public static <KI,KO,VI,VO> Map<KO,VO> transformMap(Map<? extends KI, ? extends VI> map,
                                                                 Function<KI,KO> keyTransformer,
                                                                 Function<VI,VO> valueTransformer) {
     if(map == null) {
         return null;
     }

     return map.entrySet().stream()
             .collect(Collectors.toMap(
                     entry -> keyTransformer.apply(entry.getKey()),
                     entry -> valueTransformer.apply(entry.getValue())
             ));
 }
}
