package net.intelie.challenges;

import java.util.List;

public class InMemoryEventIterator implements EventIterator{

    private final List<Event> events;
    private final InMemoryEventStore store;

    private int current = -1;

    private boolean lastMoveNextResult = false;

    public InMemoryEventIterator( InMemoryEventStore store, List<Event> events) {
        this.store = store;
        this.events = events;
    }

    @Override
    public boolean moveNext() {
        current++;
        lastMoveNextResult = current != events.size();
        return lastMoveNextResult;
    }

    @Override
    public Event current() {
        if (current == -1) throw new IllegalStateException("Current index is -1. Did you forget to call moveNext()?");
        if (!lastMoveNextResult) throw new IllegalStateException("There is no current value");
        return events.get(current);
    }

    @Override
    public void remove() {
        if (current == -1) throw new IllegalStateException("Current index is -1. Did you forget to call moveNext()?");
        if (!lastMoveNextResult) throw new IllegalStateException("There is no current value");
        store.remove(current());
    }

    @Override
    public void close() throws Exception {
        //No implementation for in memory store
    }
}
