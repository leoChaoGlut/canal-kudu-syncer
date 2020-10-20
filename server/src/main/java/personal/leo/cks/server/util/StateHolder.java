package personal.leo.cks.server.util;

import personal.leo.cks.server.constants.State;

import java.util.concurrent.atomic.AtomicReference;

public class StateHolder {
    public static final AtomicReference<State> stateRef = new AtomicReference<>(State.UNKNOWN);

    public static State state() {
        return stateRef.get();
    }

    public static boolean isActive() {
        return stateRef.get() == State.ACTIVE;
    }

    public static boolean isInactive() {
        return stateRef.get() == State.INACTIVE;
    }
}
