/**
 * Copyright 2014 Netflix, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package rx.observables;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Producer;
import rx.Subscriber;
import rx.Subscription;
import rx.annotations.Experimental;
import rx.exceptions.CompositeException;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.functions.Actions;
import rx.functions.Func0;
import rx.functions.Func2;
import rx.internal.operators.BackpressureUtils;

/**
 * Abstract base class for the {@link OnSubscribe} interface that helps you build Observable sources one
 * {@code onNext} at a time, and automatically supports unsubscription and backpressure.
 * <p>
 * <h1>Usage rules</h1>
 * When you implement the {@code next()} method, you
 * <ul>
 *  <li>should either
 *   <ul>
 *   <li>create the next value and signal it via {@link BackPressureSubscriber#onNext state.onNext()},</li>
 *   <li>signal a terminal condition via {@link BackPressureSubscriber#onError state.onError()}, or
 *       {@link BackPressureSubscriber#onCompleted state.onCompleted()}, or</li>
 *   <li>signal a stop condition via {@link BackPressureSubscriber#stop state.stop()} indicating no further values
 *       will be sent.</li>
 *   </ul>
 *  </li>
 *  <li>may
 *   <ul>
 *   <li>call {@link BackPressureSubscriber#onNext state.onNext()} and either
 *       {@link BackPressureSubscriber#onError state.onError()} or
 *       {@link BackPressureSubscriber#onCompleted state.onCompleted()} together, and
 *   <li>block or sleep.
 *   </ul>
 *  </li>
 *  <li>should not
 *   <ul>
 *   <li>do nothing or do async work and not produce any event or request stopping. If neither of 
 *       the methods are called, an {@link IllegalStateException} is forwarded to the {@code Subscriber} and
 *       the Observable is terminated;</li>
 *   <li>call the {@code state.on}<i>foo</i>() methods more than once (yields
 *       {@link IllegalStateException}).</li>
 *   </ul>
 *  </li>
 * </ul>
 * 
 * The {@link BackPressureSubscriber} object features counters that may help implement a state machine:
 * <ul>
 * <li>A call counter, accessible via {@link BackPressureSubscriber#calls state.calls()} tells how many times the
 *     {@code next()} was run (zero based).</li>
 * <li>You can use a phase counter, accessible via {@link BackPressureSubscriber#phase state.phase}, that helps track
 *     the current emission phase, in a {@code switch()} statement to implement the state machine. (It is named
 *     {@code phase} to avoid confusion with the per-subscriber state.)</li>
 * <li>You can arbitrarily change the current phase with
 *     {@link BackPressureSubscriber#advancePhase state.advancePhase()}, 
 *     {@link BackPressureSubscriber#advancePhaseBy(int) state.advancedPhaseBy(int)} and
 *     {@link BackPressureSubscriber#phase(int) state.phase(int)}.</li>
 * </ul>
 * <p>
 * When you implement {@code AbstractOnSubscribe}, you may override {@link AbstractOnSubscribe#onStart} to
 * perform special actions (such as registering {@code Subscription}s with {@code Subscriber.add()}) and return
 * additional state for each subscriber subscribing. You can access this custom state with the
 * {@link BackPressureSubscriber#state state.state()} method. If you need to do some cleanup, you can override the
 * {@link #onTerminated} method.
 * <p>
 * For convenience, a lambda-accepting static factory method, {@link #create}, is available.
 * Another convenience is {@link #toObservable} which turns an {@code AbstractOnSubscribe}
 * instance into an {@code Observable} fluently.
 * 
 * <h1>Examples</h1>
 * Note: these examples use the lambda-helper factories to avoid boilerplate.
 * 
 * <h3>Implement: just</h3>
 * <pre><code>
 * AbstractOnSubscribe.create(s -> {
 *   s.onNext(1);
 *   s.onCompleted();
 * }).toObservable().subscribe(System.out::println);
 * </code></pre>

 * <h3>Implement: from Iterable</h3>
 * <pre><code>
 * Iterable<T> iterable = ...;
 * AbstractOnSubscribe.create(s -> {
 *   Iterator<T> it = s.state();
 *   if (it.hasNext()) {
 *     s.onNext(it.next());
 *   }
 *   if (!it.hasNext()) {
 *     s.onCompleted();
 *   }
 * }, u -> iterable.iterator()).subscribe(System.out::println);
 * </code></pre>
 *
 * <h3>Implement source that fails a number of times before succeeding</h3>
 * <pre><code>
 * AtomicInteger fails = new AtomicInteger();
 * int numFails = 50;
 * AbstractOnSubscribe.create(s -> {
 *   long c = s.calls();
 *   switch (s.phase()) {
 *   case 0:
 *     s.onNext("Beginning");
 *     s.onError(new RuntimeException("Oh, failure.");
 *     if (c == numFails.getAndIncrement()) {
 *       s.advancePhase();
 *     }
 *     break;
 *   case 1:
 *     s.onNext("Beginning");
 *     s.advancePhase();
 *   case 2:
 *     s.onNext("Finally working");
 *     s.onCompleted();
 *     s.advancePhase();
 *   default:
 *     throw new IllegalStateException("How did we get here?");
 *   }
 * }).subscribe(System.out::println);
 * </code></pre>

 * <h3>Implement: never</h3>
 * <pre><code>
 * AbstractOnSubscribe.create(s -> {
 *   s.stop();
 * }).toObservable()
 * .timeout(1, TimeUnit.SECONDS)
 * .subscribe(System.out::println, Throwable::printStacktrace, () -> System.out.println("Done"));
 * </code></pre>
 *
 * @param <T> the value type
 * @param <S> the per-subscriber user-defined state type
 * @since (if this graduates from Experimental/Beta to supported, replace this parenthetical with the release number)
 * @Experimental
 */
@Experimental
public abstract class AbstractOnSubscribe<T, S> implements OnSubscribe<T> {
    protected S dataState;

    /**
     * Called when a Subscriber subscribes and lets the implementor create a per-subscriber custom state.
     * <p>
     * Override this method to have custom state per-subscriber. The default implementation returns
     * {@code null}.
     *
     * @return the custom state
     */
    protected abstract S onStart();

    /**
     * Called after the terminal emission or when the downstream unsubscribes.
     * <p>
     * This is called only once and no {@code onNext} call will run concurrently with it. The default
     * implementation does nothing.
     *
     * @param state the user-provided state
     */
    protected void onTerminated(S state) {
        
    }

    /**
     * Override this method to create an emission state-machine.
     *
     * @param subscriptionState the per-subscriber subscription state
     * @param dataState the data emitted from the call to {@link #onSubscribe(Subscriber)} (if this is 
     *          the first invocation of next) or returned by the previous invocation of next. 
     */
    protected abstract S next(BackPressureSubscriber<T> subscriptionState, S dataState);

    @Override
    public void call(final Subscriber<? super T> subscriber) {
        dataState = onStart();
        Action1<BackPressureSubscriber<T>> nextDelegator = new Action1<BackPressureSubscriber<T>>() {
            @Override
            public void call(BackPressureSubscriber<T> subscriber) {
                dataState = next(subscriber, dataState);
            }
        };
        Action0 onTerminateDelegator = new Action0 () {
                @Override
                public void call() {
                    onTerminated(dataState);
                }
            };
        SubscriptionProducer<T> producer = new SubscriptionProducer<T>(subscriber, nextDelegator, onTerminateDelegator);
        subscriber.add(new SubscriptionCompleter<T>(producer));
        subscriber.setProducer(producer);
    }
    
    /**
     * Convenience method to create an Observable from this implemented instance.
     *
     * @return the created observable
     */
    public final Observable<T> toObservable() {
        return Observable.create(this);
    }

    /** Function that returns null. */
    private static final Func0<Object> NULL_FUNC1 = new Func0<Object>() {
        @Override
        public Object call() {
            return null;
        }
    };

    /**
     * Creates an {@code AbstractOnSubscribe} instance which calls the provided {@code next} action.
     * <p>
     * This is a convenience method to help create {@code AbstractOnSubscribe} instances with the help of
     * lambdas.
     *
     * @param <T> the value type
     * @param <S> the per-subscriber user-defined state type
     * @param next the next action to call
     * @return an {@code AbstractOnSubscribe} instance
     */
    public static <T, S> AbstractOnSubscribe<T, S> create(final S initialState, Func2<BackPressureSubscriber<T>, S, S> next) {
        Func0<? extends S> onStartFunc = new Func0<S>() {
            @Override
            public S call() {
                return initialState;
            }};
        return create(next, onStartFunc, Actions.empty());
    }
    
    /**
     * Creates an {@code AbstractOnSubscribe} instance which calls the provided {@code next} action.
     * <p>
     * This is a convenience method to help create {@code AbstractOnSubscribe} instances with the help of
     * lambdas.
     *
     * @param <T> the value type
     * @param <S> the per-subscriber user-defined state type
     * @param next the next action to call
     * @return an {@code AbstractOnSubscribe} instance
     */
    public static <T, S> AbstractOnSubscribe<T, S> create(Func2<BackPressureSubscriber<T>, S, S> next) {
        @SuppressWarnings("unchecked")
        Func0<? extends S> nullFunc = (Func0<? extends S>)NULL_FUNC1;
        return create(next, nullFunc, Actions.empty());
    }
    
    /**
     * Creates an {@code AbstractOnSubscribe} instance which calls the provided {@code next} action.
     * <p>
     * This is a convenience method to help create {@code AbstractOnSubscribe} instances with the help of
     * lambdas.
     *
     * @param <T> the value type
     * @param <S> the per-subscriber user-defined state type
     * @param next the next action to call
     * @return an {@code AbstractOnSubscribe} instance
     */
    public static <T, S> AbstractOnSubscribe<T, S> create(final Action1<BackPressureSubscriber<T>> next) {
        @SuppressWarnings("unchecked")
        Func0<? extends S> nullFunc = (Func0<? extends S>)NULL_FUNC1;
        Func2<BackPressureSubscriber<T>, S, S> nextWrapper = new Func2<BackPressureSubscriber<T>, S, S>() {
            @Override
            public S call(BackPressureSubscriber<T> t1, S t2) {
                next.call(t1);
                return null;
            }};
        return create(nextWrapper, nullFunc, Actions.empty());
    }

    /**
     * Creates an {@code AbstractOnSubscribe} instance which creates a custom state with the {@code onSubscribe}
     * function and calls the provided {@code next} action.
     * <p>
     * This is a convenience method to help create {@code AbstractOnSubscribe} instances with the help of
     * lambdas.
     *
     * @param <T> the value type
     * @param <S> the per-subscriber user-defined state type
     * @param next the next action to call
     * @param onStart the function that returns a per-subscriber state to be used by {@code next}
     * @return an {@code AbstractOnSubscribe} instance
     */
    public static <T, S> AbstractOnSubscribe<T, S> create(Func2<BackPressureSubscriber<T>, S, S> next,
            Func0<? extends S> onStart) {
        return create(next, onStart, Actions.empty());
    }

    /**
     * Creates an {@code AbstractOnSubscribe} instance which creates a custom state with the {@code onSubscribe}
     * function, calls the provided {@code next} action and calls the {@code onTerminated} action to release the
     * state when its no longer needed.
     * <p>
     * This is a convenience method to help create {@code AbstractOnSubscribe} instances with the help of
     * lambdas.
     *
     * @param <T> the value type
     * @param <S> the per-subscriber user-defined state type
     * @param next the next action to call
     * @param onStart the function that returns a per-subscriber state to be used by {@code next}
     * @param onTerminated the action to call to release the state created by the {@code onSubscribe} function
     * @return an {@code AbstractOnSubscribe} instance
     */
    public static <T, S> AbstractOnSubscribe<T, S> create(Func2<BackPressureSubscriber<T>, S, S> next,
            Func0<? extends S> onStart, 
            Action1<? super S> onTerminated) {
        return new LambdaOnSubscribe<T, S>(next, onStart, onTerminated);
    }

    /**
     * An implementation that forwards the three main methods ({@code next}, {@code onSubscribe}, and
     * {@code onTermianted}) to functional callbacks.
     *
     * @param <T> the value type
     * @param <S> the per-subscriber user-defined state type
     */
    private static final class LambdaOnSubscribe<T, S> extends AbstractOnSubscribe<T, S> {
        private final Func2<BackPressureSubscriber<T>, ? super S, ? extends S> next;
        private final Func0<? extends S> onStart;
        private final Action1<? super S> onTerminated;
        private LambdaOnSubscribe(Func2<BackPressureSubscriber<T>, ? super S, ? extends S> next,
                Func0<? extends S> onSubscribe, 
                Action1<? super S> onTerminated) {
            this.next = next;
            this.onStart = onSubscribe;
            this.onTerminated = onTerminated;
        }
        @Override
        protected S onStart() {
            return dataState = onStart.call();
        }
        @Override
        protected void onTerminated(S dataState) {
            onTerminated.call(dataState);
        }
        @Override
        protected S next(BackPressureSubscriber<T> state, S dataState) {
            return next.call(state, dataState);
        }
    }

    /**
     * Manages unsubscription of the state.
     *
     * @param <T> the value type
     * @param <S> the per-subscriber user-defined state type
     */
    private static final class SubscriptionCompleter<T> extends AtomicBoolean implements Subscription {
        private static final long serialVersionUID = 7993888274897325004L;
        private final SubscriptionProducer<T> state;
        private SubscriptionCompleter(SubscriptionProducer<T> state) {
            this.state = state;
        }
        @Override
        public boolean isUnsubscribed() {
            return get();
        }
        @Override
        public void unsubscribe() {
            if (compareAndSet(false, true)) {
                state.free();
            }
        }
    }
    
    /**
     * Represents a per-subscription state for the {@code AbstractOnSubscribe} operation. It supports phasing
     * and counts the number of times a value was requested by the downstream.
     *
     * @param <T> the value type 
     * @since (if this graduates from Experimental/Beta to supported, replace this parenthetical with the release number)
     * @Experimental
     */
    public static interface BackPressureSubscriber<T> {
        /**
         * Call this method to offer the next {@code onNext} value for the subscriber.
         * 
         * @param value the value to {@code onNext}
         * @throws IllegalStateException if there is a value already offered but not taken or a terminal state
         *         is reached
         */
        public void onNext(T value);
        
        /**
         * Call this method to send an {@code onError} to the subscriber and terminate all further activities.
         * If there is a pending {@code onNext}, that value is emitted to the subscriber followed by this
         * exception.
         * 
         * @param e the exception to deliver to the client
         * @throws IllegalStateException if the terminal state has been reached already
         */
        public void onError(Throwable e);
        
        /**
         * Call this method to send an {@code onCompleted} to the subscriber and terminate all further
         * activities. If there is a pending {@code onNext}, that value is emitted to the subscriber followed by
         * this exception.
         * 
         * @throws IllegalStateException if the terminal state has been reached already
         */
        public void onCompleted();
        /**
         * Signals that there won't be any further events.
         */
        public void stop();
    }

    /**
     * Contains the producer loop that reacts to downstream requests of work.
     *
     * @param <T> the value type
     */
    private static final class SubscriptionProducer<T> implements Producer, BackPressureSubscriber<T> {
        private final AtomicLong requestCount;
        private final Action1<BackPressureSubscriber<T>> next;
        private Subscriber<? super T> subscriber;
        private final AtomicInteger inUse = new AtomicInteger(1);
        private final Action0 onTerminated;
        private T theValue;
        private boolean hasOnNext;
        private boolean hasCompleted;
        private boolean stopRequested;
        private Throwable theException;

        private SubscriptionProducer(Subscriber<? super T> subscriber, Action1<BackPressureSubscriber<T>> next, Action0 onTerminated) {
            this.subscriber = subscriber;
            this.next = next;
            this.onTerminated = onTerminated;
            this.requestCount = new AtomicLong();
        }
        
        @Override
        public void request(long n) {
            if (n > 0 && BackpressureUtils.getAndAddRequest(requestCount, n) == 0) {
                if (n == Long.MAX_VALUE) {
                    // fast-path
                    while (!subscriber.isUnsubscribed()) {
                        if (!doNext()) {
                            break;
                        }
                    }
                } else 
                if (!subscriber.isUnsubscribed()) {
                    do {
                        if (!doNext()) {
                            break;
                        }
                    } while (requestCount.decrementAndGet() > 0 && !subscriber.isUnsubscribed());
                }
            }
        }

        /**
         * Executes the user-overridden next() method and performs state bookkeeping and
         * verification.
         *
         * @return true if the outer loop may continue
         */
        protected boolean doNext() {
            if (isInUse()) // TODO should we mark that a request has been made and that the thread currently using should process the request?
                return false;
            try {
                next.call(this);
                if (!verify()) {
                    throw new IllegalStateException("No event produced or stop called when invoking generator func");
                }
                if (accept() || stopRequested()) {
                    terminate();
                    return false;
                }
            } catch (Throwable t) {
                terminate();
                subscriber.onError(t);
                return false;
            } finally {
                free();
            }
            return true;
        }
    
        @Override
        public void onNext(T value) {
            if (hasOnNext) {
                throw new IllegalStateException("onNext not consumed yet!");
            } else
            if (hasCompleted) {
                throw new IllegalStateException("Already terminated", theException);
            }
            theValue = value;
            hasOnNext = true;
        }

        @Override
        public void onError(Throwable e) {
            if (e == null) {
                throw new NullPointerException("e != null required");
            }
            if (hasCompleted) {
                throw new IllegalStateException("Already terminated", theException);
            }
            theException = e;
            hasCompleted = true;
        }

        @Override
        public void onCompleted() {
            if (hasCompleted) {
                throw new IllegalStateException("Already terminated", theException);
            }
            hasCompleted = true;
        }

        @Override
        public void stop() {
            stopRequested = true;
        }

        /**
         * Emits the {@code onNext} and/or the terminal value to the actual subscriber.
         *
         * @return {@code true} if the event was a terminal event
         */
        protected boolean accept() {
            if (hasOnNext) {
                T value = theValue;
                theValue = null;
                hasOnNext = false;
                
                try {
                    subscriber.onNext(value);
                } catch (Throwable t) {
                    hasCompleted = true;
                    Throwable e = theException;
                    theException = null;
                    if (e == null) {
                        subscriber.onError(t);
                    } else {
                        subscriber.onError(new CompositeException(Arrays.asList(t, e)));
                    }
                    return true;
                }
            }
            if (hasCompleted) {
                Throwable e = theException;
                theException = null;
                
                if (e != null) {
                    subscriber.onError(e);
                } else {
                    subscriber.onCompleted();
                }
                return true;
            }
            return false;
        }

        /**
         * Verify if the {@code next()} generated an event or requested a stop.
         *
         * @return true if either event was generated or stop was requested
         */
        protected boolean verify() {
            return hasOnNext || hasCompleted || stopRequested;
        }

        /** @return true if the {@code next()} requested a stop */
        protected boolean stopRequested() {
            return stopRequested;
        }

        /**
         * Request the state to be used by {@code onNext} or returns {@code false} if the downstream has
         * unsubscribed.
         *
         * @return {@code true} if the state can be used exclusively
         * @throws IllegalStateEception
         * @warn "throws" section incomplete
         */
        protected boolean isInUse() {
            int i = inUse.get();
            if (i == 0) {
                return true;
            } else
            if (i == 1 && inUse.compareAndSet(1, 2)) {
                return false;
            }
            throw new IllegalStateException("This is not reentrant nor threadsafe!");
        }

        /**
         * Release the state if there are no more interest in it and it is not in use.
         */
        protected void free() {
            int i = inUse.get();
            if (i <= 0) {
                return;
            } else
            if (inUse.decrementAndGet() == 0) {
                onTerminated.call();
            }
        }

        /**
         * Terminates the state immediately and calls {@link AbstractOnSubscribe#onTerminated} with the custom
         * state.
         */
        protected void terminate() {
            for (;;) {
                int i = inUse.get();
                if (i <= 0) {
                    return;
                }
                if (inUse.compareAndSet(i, 0)) {
                    onTerminated.call();
                    break;
                }
            }
        }
    }

}
