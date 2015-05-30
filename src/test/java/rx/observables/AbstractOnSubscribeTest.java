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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.mockito.InOrder;

import rx.Observable;
import rx.Observer;
import rx.Subscriber;
import rx.exceptions.TestException;
import rx.functions.Action1;
import rx.functions.Func0;
import rx.functions.Func2;
import rx.observables.AbstractOnSubscribe.BackPressureSubscriber;
import rx.observers.TestSubscriber;
import rx.schedulers.Schedulers;

/**
 * Test if AbstractOnSubscribe adheres to the usual unsubscription and backpressure contracts.
 */
public class AbstractOnSubscribeTest {
    @Test
    public void testAbstractOnSubscribeJustImplementation() {
        AbstractOnSubscribe<Integer, Void> aos = new AbstractOnSubscribe<Integer, Void>() {
            @Override
            protected Void next(BackPressureSubscriber<Integer> state, Void data) {
                state.onNext(1);
                state.onCompleted();
                return null;
            }

            @Override
            protected Void onStart() {
                return null;
            }
        };
        
        assertJustImplAOS(aos);
    }
    @Test
    public void testLambdaOnSubscribeJustImplementation() {
        AbstractOnSubscribe<Integer, Void> aos = AbstractOnSubscribe.create(new Action1<BackPressureSubscriber<Integer>>(){
            @Override
            public void call(BackPressureSubscriber<Integer> state) {
                state.onNext(1);
                state.onCompleted();
            }});
        assertJustImplAOS(aos);
    }
    private void assertJustImplAOS(AbstractOnSubscribe<Integer, Void> aos) {
        TestSubscriber<Integer> ts = new TestSubscriber<Integer>();
        
        aos.toObservable().subscribe(ts);
        
        ts.assertNoErrors();
        ts.assertTerminalEvent();
        ts.assertReceivedOnNext(Arrays.asList(1));
    }
    
    @Test
    public void testJustMisbehaving() {
        AbstractOnSubscribe<Integer, Void> aos = new AbstractOnSubscribe<Integer, Void>() {
            @Override
            protected Void next(BackPressureSubscriber<Integer> state, Void data) {
                state.onNext(1);
                state.onNext(2);
                state.onCompleted();
                return null;
            }

            @Override
            protected Void onStart() {
                return null;
            }
        };
        
        assertMisbehavingAOS(aos);
    }
    @Test
    public void testLambdaImplementationJustMisbehaving() {
        AbstractOnSubscribe<Integer, Void> aos = AbstractOnSubscribe.create(new Action1<BackPressureSubscriber<Integer>>(){
            
            @Override
            public void call(BackPressureSubscriber<Integer> state) {
                state.onNext(1);
                state.onNext(2);
                state.onCompleted();
            }});
        
        assertMisbehavingAOS(aos);
    }
    private void assertMisbehavingAOS(AbstractOnSubscribe<Integer, Void> aos) {
        @SuppressWarnings("unchecked")
        Observer<Object> o = mock(Observer.class);
        
        aos.toObservable().subscribe(o);

        verify(o, never()).onNext(any(Integer.class));
        verify(o, never()).onCompleted();
        verify(o).onError(any(IllegalStateException.class));
    }
    
    @Test
    public void testJustMisbehavingOnCompleted() {
        AbstractOnSubscribe<Integer, Void> aos = new AbstractOnSubscribe<Integer, Void>() {
            @Override
            protected Void next(BackPressureSubscriber<Integer> state, Void data) {
                state.onNext(1);
                state.onCompleted();
                state.onCompleted();
                return null;
            }

            @Override
            protected Void onStart() {
                return null;
            }
        };
        
        assertMisbehavingAOS(aos);
    }
    @Test
    public void testJustMisbehavingOnError() {
        AbstractOnSubscribe<Integer, Void> aos = new AbstractOnSubscribe<Integer, Void>() {
            @Override
            protected Void next(BackPressureSubscriber<Integer> state, Void data) {
                state.onNext(1);
                state.onError(new TestException("Forced failure 1"));
                state.onError(new TestException("Forced failure 2"));
                return null;
            }

            @Override
            protected Void onStart() {
                return null;
            }
        };
        
        assertMisbehavingAOS(aos);
    }
    
    @Test
    public void testEmpty() {
        AbstractOnSubscribe<Integer, Void> aos = new AbstractOnSubscribe<Integer, Void>() {
            @Override
            protected Void next(BackPressureSubscriber<Integer> state, Void data) {
                state.onCompleted();
                return null;
            }

            @Override
            protected Void onStart() {
                return null;
            }
        };
        
        assertEmptyImplAOS(aos);
    }
    
    @Test
    public void testLambdaImplEmpty() {
        AbstractOnSubscribe<Integer, Void> aos = AbstractOnSubscribe.create(new Action1<BackPressureSubscriber<Integer>>(){
            @Override
            public void call(BackPressureSubscriber<Integer> state) {
                state.onCompleted();
            }});
        assertEmptyImplAOS(aos);
    }
    
    private void assertEmptyImplAOS(AbstractOnSubscribe<Integer, Void> aos) {
        @SuppressWarnings("unchecked")
        Observer<Object> o = mock(Observer.class);
        
        aos.toObservable().subscribe(o);
        
        verify(o, never()).onNext(any(Integer.class));
        verify(o, never()).onError(any(Throwable.class));
        verify(o).onCompleted();
    }
    @Test
    public void testNever() {
        AbstractOnSubscribe<Integer, Void> aos = new AbstractOnSubscribe<Integer, Void>() {
            @Override
            protected Void next(BackPressureSubscriber<Integer> state, Void data) {
                state.stop();
                return null;
            }

            @Override
            protected Void onStart() {
                return null;
            }
        };
        
        assertNever(aos);
    }
    @Test
    public void testLambdaImplNever() {
        AbstractOnSubscribe<Integer, Void> aos = AbstractOnSubscribe.create(new Action1<BackPressureSubscriber<Integer>>(){
            @Override
            public void call(BackPressureSubscriber<Integer> state) {
                state.stop();
            }});
        assertNever(aos);
    }
    private void assertNever(AbstractOnSubscribe<Integer, Void> aos) {
        @SuppressWarnings("unchecked")
        Observer<Object> o = mock(Observer.class);
        
        aos.toObservable().subscribe(o);
        
        verify(o, never()).onNext(any(Integer.class));
        verify(o, never()).onError(any(Throwable.class));
        verify(o, never()).onCompleted();
    }

    @Test
    public void testThrows() {
        AbstractOnSubscribe<Integer, Void> aos = new AbstractOnSubscribe<Integer, Void>() {
            @Override
            protected Void next(BackPressureSubscriber<Integer> state, Void data) {
                throw new TestException("Forced failure");
            }

            @Override
            protected Void onStart() {
                return null;
            }
        };
        assertOnError(aos);
    }
    @Test
    public void testLambdaImplThrows() {
        AbstractOnSubscribe<Integer, Void> aos = AbstractOnSubscribe.create(new Action1<BackPressureSubscriber<Integer>>(){
            @Override
            public void call(BackPressureSubscriber<Integer> t) {
                throw new TestException("Forced failure");
            }});
        assertOnError(aos);
    }
    private void assertOnError(AbstractOnSubscribe<Integer, Void> aos) {
        @SuppressWarnings("unchecked")
        Observer<Object> o = mock(Observer.class);
        
        aos.toObservable().subscribe(o);
        
        verify(o, never()).onNext(any(Integer.class));
        verify(o, never()).onCompleted();
        verify(o).onError(any(TestException.class));
    }

    @Test
    public void testError() {
        AbstractOnSubscribe<Integer, Void> aos = new AbstractOnSubscribe<Integer, Void>() {
            @Override
            protected Void next(BackPressureSubscriber<Integer> state, Void data) {
                state.onError(new TestException("Forced failure"));
                return null;
            }

            @Override
            protected Void onStart() {
                return null;
            }
        };
        assertOnError(aos);
    }
    @Test
    public void testLambdaImplError() {
        AbstractOnSubscribe<Integer, Void> aos = AbstractOnSubscribe.create(new Action1<BackPressureSubscriber<Integer>>() {
            @Override
            public void call(BackPressureSubscriber<Integer> state) {
                state.onError(new TestException("Forced failure"));
            }});
        assertOnError(aos);
    }
    @Test
    public void testRange() {
        final int start = 1;
        final int count = 100;
        AbstractOnSubscribe<Integer, Integer> aos = new AbstractOnSubscribe<Integer, Integer>() {
            @Override
            protected Integer next(BackPressureSubscriber<Integer> state, Integer c) {
                if (c <= count) {
                    state.onNext(c);
                    if (c == count) {
                        state.onCompleted();
                    }
                }
                return c+1;
            }

            @Override
            protected Integer onStart() {
                return start;
            }
        };
        
        assertRange(start, count, aos);
    }
    @Test
    public void testLambdaImplRange() {
        final int start = 1;
        final int count = 100;
        AbstractOnSubscribe<Integer, Integer> aos = AbstractOnSubscribe.create(start,
                new Func2<BackPressureSubscriber<Integer>, Integer, Integer>() {
                    @Override
                    public Integer call(BackPressureSubscriber<Integer> state, Integer c) {
                        if (c <= count) {
                            state.onNext(c);
                            if (c == count) {
                                state.onCompleted();
                            }
                        }
                        return c+1;
                    }});
        assertRange(start, count, aos);
    }
    private void assertRange(final int start, final int count, AbstractOnSubscribe<Integer, Integer> aos) {
        @SuppressWarnings("unchecked")
        Observer<Object> o = mock(Observer.class);
        InOrder inOrder = inOrder(o);
        
        aos.toObservable().subscribe(o);
        
        verify(o, never()).onError(any(TestException.class));
        for (int i = start; i < start + count; i++) {
            inOrder.verify(o).onNext(i);
        }
        inOrder.verify(o).onCompleted();
        inOrder.verifyNoMoreInteractions();
    }
    @Test
    public void testFromIterable() {
        int n = 100;
        final List<Integer> source = new ArrayList<Integer>();
        for (int i = 0; i < n; i++) {
            source.add(i);
        }
        
        final Iterator<Integer> iterator = source.iterator();
        AbstractOnSubscribe<Integer, Iterator<Integer>> aos = new AbstractOnSubscribe<Integer, Iterator<Integer>>() {
            @Override
            protected Iterator<Integer> onStart() {
                return iterator;
            }
            @Override
            protected Iterator<Integer> next(BackPressureSubscriber<Integer> state, Iterator<Integer> data) {
                Iterator<Integer> it = iterator;
                if (it.hasNext()) {
                    state.onNext(it.next());
                }
                if (!it.hasNext()) {
                    state.onCompleted();
                }
                return data;
            }
        };
        
        @SuppressWarnings("unchecked")
        Observer<Object> o = mock(Observer.class);
        InOrder inOrder = inOrder(o);
        
        aos.toObservable().subscribe(o);
        
        verify(o, never()).onError(any(TestException.class));
        for (int i = 0; i < n; i++) {
            inOrder.verify(o).onNext(i);
        }
        inOrder.verify(o).onCompleted();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void testPhasedRetry() {
        final int count = 100;
        AbstractOnSubscribe<String, Void> aos = new AbstractOnSubscribe<String, Void>() {
            int calls;
            int phase;
            @Override
            protected Void next(BackPressureSubscriber<String> state, Void data) {
                switch (phase) {
                case 0:
                    if (calls++ < count) {
                        state.onNext("Beginning");
                        state.onError(new TestException());
                    } else {
                        phase++;
                    }
                    break;
                case 1:
                    state.onNext("Beginning");
                    phase++;
                    break;
                case 2:
                    state.onNext("Finally");
                    state.onCompleted();
                    phase++;
                    break;
                default:
                    throw new IllegalStateException("Wrong phase: " + phase);
                }
                return null;
            }
            @Override
            protected Void onStart() {
                return null;
            }
        };
        
        @SuppressWarnings("unchecked")
        Observer<Object> o = mock(Observer.class);
        InOrder inOrder = inOrder(o);
        
        aos.toObservable().retry(2 * count).subscribe(o);
        
        verify(o, never()).onError(any(Throwable.class));
        inOrder.verify(o, times(count + 1)).onNext("Beginning");
        inOrder.verify(o).onNext("Finally");
        inOrder.verify(o).onCompleted();
        inOrder.verifyNoMoreInteractions();
    }
    @Test
    public void testInfiniteTake() {
        final int start = 1;
        final int count = 100;
        AbstractOnSubscribe<Integer, Integer> aos = new AbstractOnSubscribe<Integer, Integer>() {
            @Override
            protected Integer next(BackPressureSubscriber<Integer> state, Integer data) {
                state.onNext(data);
                return data+1;
            }

            @Override
            protected Integer onStart() {
                return start;
            }
        };
        
        @SuppressWarnings("unchecked")
        Observer<Object> o = mock(Observer.class);
        InOrder inOrder = inOrder(o);
        
        aos.toObservable().take(count).subscribe(o);
        
        verify(o, never()).onError(any(Throwable.class));
        for (int i = start; i < start + count; i++) {
            inOrder.verify(o).onNext(i);
        }
        inOrder.verify(o).onCompleted();
        inOrder.verifyNoMoreInteractions();
    }
    @Test
    public void testInfiniteRequestSome() {
        final int count = 100;
        final int start = 1;
        AbstractOnSubscribe<Integer, Integer> aos = new AbstractOnSubscribe<Integer, Integer>() {
            @Override
            protected Integer next(BackPressureSubscriber<Integer> state, Integer data) {
                state.onNext(data);
                return data+1;
            }

            @Override
            protected Integer onStart() {
                return start;
            }
        };
        
        @SuppressWarnings("unchecked")
        Observer<Object> o = mock(Observer.class);
        InOrder inOrder = inOrder(o);
        
        TestSubscriber<Object> ts = new TestSubscriber<Object>(o) {
            @Override
            public void onStart() {
                requestMore(0); // don't start right away
            }
        };
        
        aos.toObservable().subscribe(ts);
        
        ts.requestMore(count);
        
        verify(o, never()).onError(any(Throwable.class));
        verify(o, never()).onCompleted();
        for (int i = start; i < start + count; i++) {
            inOrder.verify(o).onNext(i);
        }
        inOrder.verifyNoMoreInteractions();
    }
    @Test
    public void testIndependentStates() {
        int count = 100;
        final ConcurrentHashMap<Object, Object> states = new ConcurrentHashMap<Object, Object>();
        AbstractOnSubscribe<Integer, Void> aos = new AbstractOnSubscribe<Integer, Void>() {
            @Override
            protected Void next(BackPressureSubscriber<Integer> state, Void data) {
                states.put(state, state);
                state.stop();
                return null;
            }

            @Override
            protected Void onStart() {
                return null;
            }
        };
        Observable<Integer> source = aos.toObservable();
        for (int i = 0; i < count; i++) {
            source.subscribe();
        }
        
        assertEquals(count, states.size());
    }
    @Test(timeout = 3000)
    public void testSubscribeOn() {
        final int start = 1;
        final int count = 100;
        AbstractOnSubscribe<Integer, Integer> aos = new AbstractOnSubscribe<Integer, Integer>() {
            @Override
            protected Integer next(BackPressureSubscriber<Integer> state, Integer data) {
                if (data <= count) {
                    state.onNext(data);
                    if (data == count) {
                        state.onCompleted();
                    }
                }
                return data+1;
            }

            @Override
            protected Integer onStart() {
                return start;
            }
        };

        @SuppressWarnings("unchecked")
        Observer<Object> o = mock(Observer.class);
        InOrder inOrder = inOrder(o);

        TestSubscriber<Object> ts = new TestSubscriber<Object>(o);

        aos.toObservable().subscribeOn(Schedulers.newThread()).subscribe(ts);
        
        ts.awaitTerminalEvent();
        
        verify(o, never()).onError(any(Throwable.class));
        for (int i = start; i < start + count; i++) {
            inOrder.verify(o).onNext(i);
        }
        inOrder.verify(o).onCompleted();
        inOrder.verifyNoMoreInteractions();

    }
    @Test(timeout = 10000)
    public void testObserveOn() {
        final int count = 1000;
        final int start = 1;
        AbstractOnSubscribe<Integer, Integer> aos = new AbstractOnSubscribe<Integer, Integer>() {
            @Override
            protected Integer next(BackPressureSubscriber<Integer> state, Integer data) {
                if (data - start <= count) {
                    state.onNext(data);
                    if (data == count) {
                        state.onCompleted();
                    }
                }
                return data+1;
            }

            @Override
            protected Integer onStart() {
                return start;
            }
        };

        @SuppressWarnings("unchecked")
        Observer<Object> o = mock(Observer.class);

        TestSubscriber<Object> ts = new TestSubscriber<Object>(o);

        aos.toObservable().observeOn(Schedulers.newThread()).subscribe(ts);
        
        ts.awaitTerminalEvent();
        
        verify(o, never()).onError(any(Throwable.class));
        verify(o, times(count)).onNext(any(Integer.class));
        verify(o).onCompleted();
        
        List<Object> onNextEvents = ts.getOnNextEvents();
        for (int i = 0; i < onNextEvents.size(); i++) {
            Object object = onNextEvents.get(i);
            assertEquals(i + 1, object);
        }
    }
    
    @Test
    public void testCanRequestInOnNext() {
        AbstractOnSubscribe<Integer, Void> aos = new AbstractOnSubscribe<Integer, Void>() {
            @Override
            protected Void next(BackPressureSubscriber<Integer> state, Void data) {
                state.onNext(1);
                state.onCompleted();
                return null;
            }

            @Override
            protected Void onStart() {
                return null;
            }
        };
        final AtomicReference<Throwable> exception = new AtomicReference<Throwable>();
        aos.toObservable().subscribe(new Subscriber<Integer>() {

            @Override
            public void onCompleted() {

            }

            @Override
            public void onError(Throwable e) {
                exception.set(e);
            }

            @Override
            public void onNext(Integer t) {
                request(1);
            }
        });
        if (exception.get()!=null) {
            exception.get().printStackTrace();
        }
        assertNull(exception.get());
    }
/*
    @Test
    public void testLambdaImplMultipleOnNext() {
        final int start = 1;
        final int count = 100;
        AbstractOnSubscribe<Integer, Integer> aos = AbstractOnSubscribe.create(start,
                new Func2<BackPressureSubscriber<Integer>, Integer, Integer>() {
                    @Override
                    public Integer call(BackPressureSubscriber<Integer> subscriber, Integer c) {
                        if (c <= count) {
                            subscriber.onNext(c);
                            subscriber.onNext(c+1);
                            subscriber.onNext(c+2);
                            if (c == count) {
                                subscriber.onCompleted();
                            }
                        }
                        return c+3;
                    }});
        assertRange(start, count, aos);
    }
*/
}
