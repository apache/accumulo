package cloudtrace.instrument;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

public class TraceProxy {
	//private static final Logger log = Logger.getLogger(TraceProxy.class);

	static final Sampler ALWAYS = new Sampler() {
		@Override
		public boolean next() {
			return true;
		}
	};

	public static <T> T trace(T instance) {
        return trace(instance, ALWAYS);
    }

	@SuppressWarnings("unchecked")
	public static <T> T trace(final T instance, final Sampler sampler) {
		InvocationHandler handler = new InvocationHandler() {
			@Override
			public Object invoke(Object obj, Method method, Object[] args)
					throws Throwable {
				if (!sampler.next()) {
					return method.invoke(instance, args);
				}
				Span span = Trace.on(method.getName());
				try {
					return method.invoke(instance, args);
				} catch (Throwable ex) {
					ex.printStackTrace();
					throw ex;
				} finally {
					span.stop();
				}
			}
		};
		return (T) Proxy.newProxyInstance(instance.getClass().getClassLoader(),
				instance.getClass().getInterfaces(), handler);
	}

}
