package rx;

import rx.Observable.OnSubscribe;

/**
 * Converts the values emitted by an Observable's OnSubscribe function to a value. 
 *
 * @param <T> the type of values to be consumed
 * @param <R> the return type
 */
public interface ConversionFunc<T, R> {
    /**
     * Converts the data produced by the provided {@code OnSubscribe function} to a value.
     * 
     * @param onSubscribe a function that produces data to a Subscriber, usually wrapped by an Observable.
     * @return an instance of {@code R}
     */
    public R convert(OnSubscribe<T> onSubscribe);
}
