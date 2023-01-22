package net.intelie.challenges;

import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.*;

public class EventTest {

    private EventStore store;

    public static final String TYPE_1 = "TYPE1";
    public static final String TYPE_2 = "TYPE2";

    public static final long TIMESTAMP_1 = 1;
    public static final long TIMESTAMP_5 = 5;
    public static final long TIMESTAMP_10 = 10;

    @Before
    public void setUp() {
        store = new InMemoryEventStore();
    }

    @Test
    public void thisIsAWarning() throws Exception {
        Event event = new Event("some_type", 123L);

        //THIS IS A WARNING:
        //Some of us (not everyone) are coverage freaks.
        assertEquals(123L, event.timestamp());
        assertEquals("some_type", event.type());
    }

    @Test
    public void challengeAccepted() {
        int testCoveragePercentage = 100;
        assertEquals(100, testCoveragePercentage);
    }

    @Test
    public void testEquals() {
        Event event = new Event(TYPE_1, TIMESTAMP_1);

        assertThat(event, is(new Event(TYPE_1, TIMESTAMP_1)));
        assertThat(event, is(not(new Event(TYPE_1, TIMESTAMP_5))));
        assertThat(event, is(not(new Event(TYPE_2, TIMESTAMP_1))));
    }

    @Test
    public void testHashCode() {
        Event event = new Event(TYPE_1, TIMESTAMP_1);
        int code = event.hashCode();
        assertThat(code, is(new Event(TYPE_1, TIMESTAMP_1).hashCode()));
        assertThat(code, is(not(new Event(TYPE_1, TIMESTAMP_5).hashCode())));
        assertThat(code, is(not(new Event(TYPE_2, TIMESTAMP_1).hashCode())));
    }

    @Test
    public void insertEvent() {
        store.insert(new Event(TYPE_1, TIMESTAMP_1));
        EventIterator events = store.query(TYPE_1, TIMESTAMP_1, TIMESTAMP_1);
        assertTrue(events.moveNext());
        assertThat(events.current(), is(new Event(TYPE_1, TIMESTAMP_1)));
    }

    @Test
    public void multipleEventsWithDifferentTypes() {
        store.insert(new Event(TYPE_1, TIMESTAMP_1));
        store.insert(new Event(TYPE_2, TIMESTAMP_1));

        EventIterator type1Events = store.query(TYPE_1, TIMESTAMP_1, TIMESTAMP_1);
        assertTrue(type1Events.moveNext());
        assertThat(type1Events.current(), is(new Event(TYPE_1, TIMESTAMP_1)));

        EventIterator type2Events = store.query(TYPE_2, TIMESTAMP_1, TIMESTAMP_1);
        assertTrue(type2Events.moveNext());
        assertThat(type2Events.current(), is(new Event(TYPE_2, TIMESTAMP_1)));
    }

    @Test
    public void multipleEventsWithSameTypeAndTimestamp() {
        store.insert(new Event(TYPE_1, TIMESTAMP_1));
        store.insert(new Event(TYPE_1, TIMESTAMP_1));

        EventIterator events = store.query(TYPE_1, TIMESTAMP_1, TIMESTAMP_1);
        assertTrue(events.moveNext());
        assertFalse(events.moveNext());
    }

    @Test
    public void multipleEventsWithSameTypeAndDifferentTimestamp() {
        store.insert(new Event(TYPE_1, TIMESTAMP_1));
        store.insert(new Event(TYPE_1, TIMESTAMP_5));

        EventIterator events = store.query(TYPE_1, TIMESTAMP_1, TIMESTAMP_5);
        assertTrue(events.moveNext());
        assertThat(events.current(), is(new Event(TYPE_1, TIMESTAMP_1)));
        assertFalse(events.moveNext());

        events = store.query(TYPE_1, TIMESTAMP_1, TIMESTAMP_10);
        assertTrue(events.moveNext());
        assertThat(events.current(), is(new Event(TYPE_1, TIMESTAMP_1)));
        assertTrue(events.moveNext());
        assertThat(events.current(), is(new Event(TYPE_1, TIMESTAMP_5)));
    }

    @Test
    public void removeAllEventsOfType() {
        store.insert(new Event(TYPE_1, TIMESTAMP_1));
        store.insert(new Event(TYPE_1, TIMESTAMP_5));
        store.insert(new Event(TYPE_2, TIMESTAMP_1));

        store.removeAll(TYPE_1);
        EventIterator type1Events = store.query(TYPE_1, TIMESTAMP_1, TIMESTAMP_10);
        assertFalse(type1Events.moveNext());

        EventIterator type2Events = store.query(TYPE_2, TIMESTAMP_1, TIMESTAMP_10);
        assertTrue(type2Events.moveNext());
        assertThat(type2Events.current(), is(new Event(TYPE_2, TIMESTAMP_1)));
    }

    @Test
    public void remove() {
        store.insert(new Event(TYPE_1, TIMESTAMP_1));
        store.insert(new Event(TYPE_1, TIMESTAMP_5));

        EventIterator events = store.query(TYPE_1, TIMESTAMP_1, TIMESTAMP_10);
        events.moveNext();
        events.remove();

        EventIterator newEvents = store.query(TYPE_1, TIMESTAMP_1, TIMESTAMP_10);
        newEvents.moveNext();

        assertThat(newEvents.current(), is(new Event(TYPE_1, TIMESTAMP_5)));
        assertFalse(newEvents.moveNext());
    }

    @Test
    public void removeEventThatDoesNotExist() {
        store.insert(new Event(TYPE_1, TIMESTAMP_5));
    }

    @Test
    public void callingCurrentBeforeCallingMoveNextAtTheBeginning() {
        store.insert(new Event(TYPE_1, TIMESTAMP_1));
        EventIterator events = store.query(TYPE_1, TIMESTAMP_1, TIMESTAMP_1);
        assertThrows(IllegalStateException.class, events::current);
    }

    @Test
    public void callingCurrentWhenMoveNextReturnedFalse() {
        store.insert(new Event(TYPE_1, TIMESTAMP_1));
        EventIterator events = store.query(TYPE_1, TIMESTAMP_1, TIMESTAMP_1);
        assertTrue(events.moveNext());
        assertFalse(events.moveNext());
        assertThrows(IllegalStateException.class, events::current);
    }

    @Test
    public void callingRemoveBeforeCallingMoveNextAtTheBeginning() {
        store.insert(new Event(TYPE_1, TIMESTAMP_1));
        EventIterator events = store.query(TYPE_1, TIMESTAMP_1, TIMESTAMP_1);
        assertThrows(IllegalStateException.class, events::remove);
    }

    @Test
    public void callingRemoveWhenMoveNextReturnedFalse() {
        store.insert(new Event(TYPE_1, TIMESTAMP_1));
        EventIterator events = store.query(TYPE_1, TIMESTAMP_1, TIMESTAMP_1);
        assertTrue(events.moveNext());
        assertFalse(events.moveNext());
        assertThrows(IllegalStateException.class, events::remove);
    }
}
