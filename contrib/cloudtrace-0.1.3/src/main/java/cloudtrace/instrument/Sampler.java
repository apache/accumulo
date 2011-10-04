package cloudtrace.instrument;

/**
 * Extremely simple callback to determine the frequency that an action should be performed.
 * @see Trace.wrapAll
 */
public interface Sampler {
	
	boolean next();

}
