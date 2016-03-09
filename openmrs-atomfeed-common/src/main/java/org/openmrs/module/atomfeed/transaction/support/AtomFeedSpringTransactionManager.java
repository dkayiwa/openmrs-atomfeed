package org.openmrs.module.atomfeed.transaction.support;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.hibernate.SessionFactory;
import org.ict4h.atomfeed.jdbc.JdbcConnectionProvider;
import org.ict4h.atomfeed.transaction.AFTransactionManager;
import org.ict4h.atomfeed.transaction.AFTransactionWork;
import org.openmrs.api.context.ServiceContext;
import org.springframework.context.ApplicationContext;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.ReflectionUtils;

public class AtomFeedSpringTransactionManager implements AFTransactionManager, JdbcConnectionProvider {
    private PlatformTransactionManager transactionManager;
    private Map<AFTransactionWork.PropagationDefinition, Integer> propagationMap = new HashMap<AFTransactionWork.PropagationDefinition, Integer>();

    public AtomFeedSpringTransactionManager(PlatformTransactionManager transactionManager) {
        this.transactionManager = transactionManager;
        propagationMap.put(AFTransactionWork.PropagationDefinition.PROPAGATION_REQUIRED, TransactionDefinition.PROPAGATION_REQUIRED);
        propagationMap.put(AFTransactionWork.PropagationDefinition.PROPAGATION_REQUIRES_NEW, TransactionDefinition.PROPAGATION_REQUIRES_NEW);
    }

    @Override
    public <T> T executeWithTransaction(final AFTransactionWork<T> action) throws RuntimeException {
        TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);
        Integer txPropagationDef = getTxPropagation(action.getTxPropagationDefinition());
        transactionTemplate.setPropagationBehavior(txPropagationDef);
        return transactionTemplate.execute( new TransactionCallback<T>() {
            @Override
            public T doInTransaction(TransactionStatus status) {
                return action.execute();
            }
        });
    }

    private Integer getTxPropagation(AFTransactionWork.PropagationDefinition propagationDefinition) {
        return propagationMap.get(propagationDefinition);
    }

    /**
     * @see org.ict4h.atomfeed.jdbc.JdbcConnectionProvider
     * @return
     * @throws SQLException
     */
    @Override
    public Connection getConnection() throws SQLException {
        //TODO: ensure that only connection associated with current thread current transaction is given
    	try {
    		org.hibernate.Session session = getSession();
            Method connectionMethod = session.getClass().getMethod("connection");
            return (Connection) ReflectionUtils.invokeMethod(connectionMethod, session);
        }
        catch (NoSuchMethodException ex) {
            throw new IllegalStateException("Cannot find connection() method on Hibernate session", ex);
        }
    }

    private org.hibernate.Session getSession() {
        ServiceContext serviceContext = ServiceContext.getInstance();
        Class klass = serviceContext.getClass();
        try {
            Field field = klass.getDeclaredField("applicationContext");
            field.setAccessible(true);
            ApplicationContext applicationContext = (ApplicationContext) field.get(serviceContext);
            SessionFactory factory = (SessionFactory) applicationContext.getBean("sessionFactory");
            return getCurrentSession(factory);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
	 * Gets the current hibernate session while taking care of the hibernate 3 and 4 differences.
	 * 
	 * @return the current hibernate session.
	 */
	private org.hibernate.Session getCurrentSession(SessionFactory sessionFactory) {
		try {
			return sessionFactory.getCurrentSession();
		}
		catch (NoSuchMethodError ex) {
			try {
				Method method = sessionFactory.getClass().getMethod("getCurrentSession", null);
				return (org.hibernate.Session)method.invoke(sessionFactory, null);
			}
			catch (Exception e) {
				throw new RuntimeException("Failed to get the current hibernate session", e);
			}
		}
	}
}
