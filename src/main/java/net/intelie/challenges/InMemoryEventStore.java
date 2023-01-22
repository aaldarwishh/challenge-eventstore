package net.intelie.challenges;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryEventStore implements EventStore {

    //The store is implemented with a ConcurrentHashMap. It provides thread safety without locking.
    //Retrievals will get most recent completed writes which is reasonable.
    //The type is used as key since the current operations always filter by type first.
    //A set is used for timestamps of the events for fast retrieval. This assumes no duplicate events of the same type and time
    //The advantages of this solution is that it is simple and things like thread safety is handled for us. One disadvantage is
    //that it is specialized for the current requirements and might not be flexible or performant in other cases.
    private final Map<String, NavigableSet<Long>> store = new ConcurrentHashMap<>();

    @Override
    public void insert(Event event) {
        if(store.containsKey(event.type())) {
            store.get(event.type()).add(event.timestamp());
        } else {
            final TreeSet<Long> set = new TreeSet<>();
            set.add(event.timestamp());
            store.put(event.type(), set);
        }
    }

    @Override
    public void removeAll(String type) {
        store.remove(type);
    }

    @Override
    public EventIterator query(String type, long startTime, long endTime) {
        NavigableSet<Long> eventTimestamps = store.get(type);
        if (eventTimestamps == null) return new InMemoryEventIterator(this, Collections.emptyList());

        //Using the NavigableSet make the code simple as the needed operations are already implemented
        if (startTime == endTime) endTime++; //if startTime = endTime add one to endTime since subSet require it when toInclusive is false
        eventTimestamps = eventTimestamps.subSet(startTime, true, endTime, false);

        List<Event> events = new ArrayList<>();

        for (Long timestamp: eventTimestamps) {
            events.add(new Event(type, timestamp));
        }

        return new InMemoryEventIterator(this, events);
    }

    public void remove(Event event) {
        NavigableSet<Long> eventTimestamps = store.get(event.type());
        eventTimestamps.remove(event.timestamp());
    }
}
