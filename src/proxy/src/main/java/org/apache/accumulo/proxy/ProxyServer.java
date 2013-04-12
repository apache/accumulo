package org.apache.accumulo.proxy;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.security.thrift.SecurityErrorCode;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.accumulo.proxy.thrift.*;
import org.apache.accumulo.proxy.thrift.IteratorSetting;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Proxy Server exposing the Accumulo API via Thrift..
 * @since 1.5, backported to 1.4.4
 */
public class ProxyServer implements AccumuloProxy.Iface {

    public static final Logger logger = Logger.getLogger(ProxyServer.class);
    protected Instance instance;

    static protected class ScannerPlusIterator {
        public ScannerBase scanner;
        public Iterator<Map.Entry<org.apache.accumulo.core.data.Key,Value>> iterator;
    }

    static class CloseWriter implements RemovalListener<UUID,BatchWriter> {
        @Override
        public void onRemoval(RemovalNotification<UUID,BatchWriter> notification) {
            try {
                notification.getValue().close();
            } catch (org.apache.accumulo.core.client.MutationsRejectedException e) {
                logger.warn(e, e);
            }
        }

        public CloseWriter() {}
    }

    static class CloseScanner implements RemovalListener<UUID,ScannerPlusIterator> {
        @Override
        public void onRemoval(RemovalNotification<UUID,ScannerPlusIterator> notification) {
            final ScannerBase base = notification.getValue().scanner;
            if (base instanceof BatchScanner) {
                final BatchScanner scanner = (BatchScanner) base;
                scanner.close();
            }
        }

        public CloseScanner() {}
    }

    protected Cache<UUID,ScannerPlusIterator> scannerCache;
    protected Cache<UUID,BatchWriter> writerCache;

    protected final String PASSWORD_PROP = "password";

    public ProxyServer(Properties props) {

        String useMock = props.getProperty("org.apache.accumulo.proxy.ProxyServer.useMockInstance");
        if (useMock != null && Boolean.parseBoolean(useMock))
            instance = new MockInstance(props.getProperty(getClass().getName() + ".instancename"), new LocalFileSystem());
        else
            instance = new ZooKeeperInstance(props.getProperty(getClass().getName() + ".instancename"),
                    props.getProperty(getClass().getName() + ".zookeepers"));

        scannerCache = CacheBuilder.newBuilder().expireAfterAccess(10, TimeUnit.MINUTES).maximumSize(1000).removalListener(new CloseScanner()).build();

        writerCache = CacheBuilder.newBuilder().expireAfterAccess(10, TimeUnit.MINUTES).maximumSize(1000).removalListener(new CloseWriter()).build();
    }

    protected Connector getConnector(ByteBuffer login) throws
            AccumuloException,
            AccumuloSecurityException {

        AuthInfo user = CredentialHelper.fromByteArray(login.array());

        if (user == null)
            throw new AccumuloSecurityException("unknown user", SecurityErrorCode.BAD_CREDENTIALS);

        Connector connector = instance.getConnector(user.getUser(), user.getPassword());
        return connector;
    }

    protected ByteBuffer encodeUserPrincipal(String principal, String token) throws AccumuloSecurityException {
        return ByteBuffer.wrap(CredentialHelper.asByteArray(
                new AuthInfo(principal, ByteBuffer.wrap(token.getBytes()), instance.getInstanceID())));
    }

    protected void throwNotImplemented() throws TException {
        throw new TException("This feature is not implemented in this version of Accumulo");
    }

    @Override
    public ByteBuffer login(String principal, Map<String, String> loginProperties)
            throws org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {
        try {

            // We don't have an Authenticator on the instance in 1.4- pulling "password" prop manually
            String token = loginProperties.get(PASSWORD_PROP);
            instance.getConnector(principal, token);

            return encodeUserPrincipal(principal, token);
        } catch (Exception e) {
            throw new org.apache.accumulo.proxy.thrift.AccumuloSecurityException(e.toString());
        }
    }

    @Override
    public int addConstraint(ByteBuffer login, String tableName, String constraintClassName) throws
            org.apache.accumulo.proxy.thrift.AccumuloException,
            org.apache.accumulo.proxy.thrift.AccumuloSecurityException,
            org.apache.accumulo.proxy.thrift.TableNotFoundException, TException {

        try {

            Connector connector = getConnector(login);

            TreeSet<Integer> constraintNumbers = new TreeSet<Integer>();
            TreeMap<String,Integer> constraintClasses = new TreeMap<String,Integer>();
            int i;
            for (Map.Entry<String,String> property : connector.tableOperations().getProperties(tableName)) {
                if (property.getKey().startsWith(Property.TABLE_CONSTRAINT_PREFIX.toString())) {
                    try {
                        i = Integer.parseInt(property.getKey().substring(Property.TABLE_CONSTRAINT_PREFIX.toString().length()));
                    } catch (NumberFormatException e) {
                        throw new org.apache.accumulo.proxy.thrift.AccumuloException("Bad key for existing constraint: " + property.toString());
                    }
                    constraintNumbers.add(i);
                    constraintClasses.put(property.getValue(), i);
                }
            }

            i = 1;
            while (constraintNumbers.contains(i))
                i++;
            if (constraintClasses.containsKey(constraintClassName))
                throw new AccumuloException("Constraint " + constraintClassName + " already exists for table " + tableName + " with number "
                        + constraintClasses.get(constraintClassName));
            connector.tableOperations().setProperty(tableName, Property.TABLE_CONSTRAINT_PREFIX.toString() + i, constraintClassName);
            return i;
        } catch (AccumuloException e) {
            throw new org.apache.accumulo.proxy.thrift.AccumuloException(e.toString());
        } catch (AccumuloSecurityException e) {
            throw new org.apache.accumulo.proxy.thrift.AccumuloSecurityException(e.toString());
        } catch (TableNotFoundException e) {
            throw new org.apache.accumulo.proxy.thrift.TableNotFoundException(e.toString());
        } catch (Exception e) {
            throw new TException(e.toString());
        }
}

    @Override
    public void addSplits(ByteBuffer login, String tableName, Set<ByteBuffer> splits) throws
            org.apache.accumulo.proxy.thrift.AccumuloException,
            org.apache.accumulo.proxy.thrift.AccumuloSecurityException,
            org.apache.accumulo.proxy.thrift.TableNotFoundException, TException {

        try {
            SortedSet<Text> sorted = new TreeSet<Text>();
            for (ByteBuffer split : splits) {
                sorted.add(ByteBufferUtil.toText(split));
            }
            getConnector(login).tableOperations().addSplits(tableName, sorted);
        } catch (AccumuloException e) {
            throw new org.apache.accumulo.proxy.thrift.AccumuloException(e.toString());
        } catch(AccumuloSecurityException e)  {
            throw new org.apache.accumulo.proxy.thrift.AccumuloSecurityException(e.toString());
        } catch(TableNotFoundException e) {
            throw new org.apache.accumulo.proxy.thrift.TableNotFoundException(e.toString());
        } catch (Exception e) {
            throw new TException(e.toString());
        }
    }

    @Override
    public void attachIterator(ByteBuffer login, String tableName, IteratorSetting setting, Set<IteratorScope> scopes) throws
            org.apache.accumulo.proxy.thrift.AccumuloSecurityException,
            org.apache.accumulo.proxy.thrift.AccumuloException,
            org.apache.accumulo.proxy.thrift.TableNotFoundException, TException {

        try {
            getConnector(login).tableOperations().attachIterator(tableName, getIteratorSetting(setting), getIteratorScopes(scopes));
        } catch (AccumuloException e) {
            throw new org.apache.accumulo.proxy.thrift.AccumuloException(e.toString());
        } catch(AccumuloSecurityException e)  {
            throw new org.apache.accumulo.proxy.thrift.AccumuloSecurityException(e.toString());
        } catch(TableNotFoundException e) {
            throw new org.apache.accumulo.proxy.thrift.TableNotFoundException(e.toString());
        } catch (Exception e) {
            throw new TException(e.toString());
        }
    }

    @Override
    public void checkIteratorConflicts(ByteBuffer login, String tableName, IteratorSetting setting, Set<IteratorScope> scopes) throws
        org.apache.accumulo.proxy.thrift.AccumuloException,
        org.apache.accumulo.proxy.thrift.TableNotFoundException, TException {

        try {
            getConnector(login).tableOperations().checkIteratorConflicts(tableName, getIteratorSetting(setting), getIteratorScopes(scopes));
        } catch (AccumuloException e) {
            throw new org.apache.accumulo.proxy.thrift.AccumuloException(e.toString());
        } catch(AccumuloSecurityException e)  {
            throw new org.apache.accumulo.proxy.thrift.AccumuloException(e.toString());
        } catch(TableNotFoundException e) {
            throw new org.apache.accumulo.proxy.thrift.TableNotFoundException(e.toString());
        } catch (Exception e) {
            throw new org.apache.accumulo.proxy.thrift.AccumuloException(e.toString());
        }
    }

    @Override
    public void clearLocatorCache(ByteBuffer login, String tableName) throws
            org.apache.accumulo.proxy.thrift.TableNotFoundException, TException {

        try {
            getConnector(login).tableOperations().clearLocatorCache(tableName);
        } catch (AccumuloException e) {
            throw new TException(e.toString());
        } catch(AccumuloSecurityException e)  {
            throw new TException(e.toString());
        } catch(TableNotFoundException e) {
            throw new org.apache.accumulo.proxy.thrift.TableNotFoundException(e.toString());
        } catch (Exception e) {
            throw new TException(e.toString());
        }
    }

    @Override
    public void cloneTable(ByteBuffer login, String tableName, String newTableName, boolean flush, Map<String, String> propertiesToSet, Set<String> propertiesToExclude)
            throws org.apache.accumulo.proxy.thrift.AccumuloException,
            org.apache.accumulo.proxy.thrift.AccumuloSecurityException,
            org.apache.accumulo.proxy.thrift.TableNotFoundException,
            org.apache.accumulo.proxy.thrift.TableExistsException, TException {

        try {

            if(propertiesToExclude == null) {
                propertiesToExclude = new HashSet<String>();
            }

            if(propertiesToSet == null) {
                propertiesToSet = new HashMap<String,String>();
            }

            getConnector(login).tableOperations().clone(tableName, newTableName, flush, propertiesToSet, propertiesToExclude);
        } catch (AccumuloException e) {
            throw new org.apache.accumulo.proxy.thrift.AccumuloException(e.toString());
        } catch(AccumuloSecurityException e)  {
            throw new org.apache.accumulo.proxy.thrift.AccumuloSecurityException(e.toString());
        } catch(TableNotFoundException e) {
            throw new org.apache.accumulo.proxy.thrift.TableNotFoundException(e.toString());
        } catch(TableExistsException e) {
            throw new org.apache.accumulo.proxy.thrift.TableExistsException(e.toString());
        } catch(Exception e) {
            throw new TException(e.toString());
        }
    }

    @Override
    public void compactTable(ByteBuffer login, String tableName, ByteBuffer startRow, ByteBuffer endRow, List<IteratorSetting> iterators, boolean flush, boolean wait) throws
            org.apache.accumulo.proxy.thrift.AccumuloSecurityException,
            org.apache.accumulo.proxy.thrift.TableNotFoundException,
            org.apache.accumulo.proxy.thrift.AccumuloException, TException {
        try {
            getConnector(login).tableOperations().compact(tableName, ByteBufferUtil.toText(startRow), ByteBufferUtil.toText(endRow), flush, wait);
        } catch (AccumuloException e) {
            throw new org.apache.accumulo.proxy.thrift.AccumuloException(e.toString());
        } catch(AccumuloSecurityException e)  {
            throw new org.apache.accumulo.proxy.thrift.AccumuloSecurityException(e.toString());
        } catch(TableNotFoundException e) {
            throw new org.apache.accumulo.proxy.thrift.TableNotFoundException(e.toString());
        } catch (Exception e) {
            throw new TException(e.toString());
        }
    }

    @Override
    public void createTable(ByteBuffer login, String tableName, boolean versioningIter, TimeType type) throws
            org.apache.accumulo.proxy.thrift.AccumuloException,
            org.apache.accumulo.proxy.thrift.AccumuloSecurityException,
            org.apache.accumulo.proxy.thrift.TableExistsException, TException {


        try {
            if (type == null)
                type = org.apache.accumulo.proxy.thrift.TimeType.MILLIS;

            getConnector(login).tableOperations().create(tableName, versioningIter, org.apache.accumulo.core.client.admin.TimeType.valueOf(type.toString()));
        } catch (AccumuloException e) {
            throw new org.apache.accumulo.proxy.thrift.AccumuloException(e.toString());
        } catch(AccumuloSecurityException e)  {
            throw new org.apache.accumulo.proxy.thrift.AccumuloSecurityException(e.toString());
        } catch(TableExistsException e) {
            throw new org.apache.accumulo.proxy.thrift.TableExistsException(e.toString());
        } catch (Exception e) {
            throw new TException(e.toString());
        }
    }

    @Override
    public void deleteTable(ByteBuffer login, String tableName) throws
            org.apache.accumulo.proxy.thrift.AccumuloException,
            org.apache.accumulo.proxy.thrift.AccumuloSecurityException,
            org.apache.accumulo.proxy.thrift.TableNotFoundException, TException {

        try {
            getConnector(login).tableOperations().delete(tableName);
        } catch (AccumuloException e) {
            throw new org.apache.accumulo.proxy.thrift.AccumuloException(e.toString());
        } catch(AccumuloSecurityException e)  {
            throw new org.apache.accumulo.proxy.thrift.AccumuloSecurityException(e.toString());
        } catch(TableNotFoundException e) {
            throw new org.apache.accumulo.proxy.thrift.TableNotFoundException(e.toString());
        } catch (Exception e) {
            throw new TException(e.toString());
        }
    }

    @Override
    public void deleteRows(ByteBuffer login, String tableName, ByteBuffer startRow, ByteBuffer endRow) throws
            org.apache.accumulo.proxy.thrift.AccumuloException,
            org.apache.accumulo.proxy.thrift.AccumuloSecurityException,
            org.apache.accumulo.proxy.thrift.TableNotFoundException, TException {
        try {
            getConnector(login).tableOperations().deleteRows(tableName, ByteBufferUtil.toText(startRow), ByteBufferUtil.toText(endRow));
        } catch (AccumuloException e) {
            throw new org.apache.accumulo.proxy.thrift.AccumuloException(e.toString());
        } catch(AccumuloSecurityException e)  {
            throw new org.apache.accumulo.proxy.thrift.AccumuloSecurityException(e.toString());
        } catch(TableNotFoundException e) {
            throw new org.apache.accumulo.proxy.thrift.TableNotFoundException(e.toString());
        } catch (Exception e) {
            throw new TException(e.toString());
        }
    }

    @Override
    public void flushTable(ByteBuffer login, String tableName, ByteBuffer startRow, ByteBuffer endRow, boolean wait) throws
            org.apache.accumulo.proxy.thrift.AccumuloException,
            org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {
        try {
            getConnector(login).tableOperations().flush(tableName, ByteBufferUtil.toText(startRow), ByteBufferUtil.toText(endRow), wait);
        } catch (AccumuloException e) {
            throw new org.apache.accumulo.proxy.thrift.AccumuloException(e.toString());
        } catch(AccumuloSecurityException e)  {
            throw new org.apache.accumulo.proxy.thrift.AccumuloSecurityException(e.toString());
        } catch (Exception e) {
            throw new TException(e.toString()); //TODO: should probably throw notFoundException in this method
        }
    }

    @Override
    public Map<String, Set<String>> getLocalityGroups(ByteBuffer login, String tableName) throws
            org.apache.accumulo.proxy.thrift.AccumuloException,
            org.apache.accumulo.proxy.thrift.TableNotFoundException, TException {
        try {
            Map<String,Set<Text>> groups = getConnector(login).tableOperations().getLocalityGroups(tableName);
            Map<String,Set<String>> ret = new HashMap<String,Set<String>>();
            for (String key : groups.keySet()) {
                ret.put(key, new HashSet<String>());
                for (Text val : groups.get(key)) {
                    ret.get(key).add(val.toString());
                }
            }
            return ret;
        } catch (AccumuloException e) {
            throw new org.apache.accumulo.proxy.thrift.AccumuloException(e.toString());
        } catch(AccumuloSecurityException e)  {
            throw new org.apache.accumulo.proxy.thrift.AccumuloException(e.toString()); // TODO: Should this be securityException?
        } catch(TableNotFoundException e) {
            throw new org.apache.accumulo.proxy.thrift.TableNotFoundException(e.toString());
        } catch (Exception e) {
            throw new TException(e.toString());
        }
    }

    @Override
    public IteratorSetting getIteratorSetting(ByteBuffer login, String tableName, String iteratorName, IteratorScope scope) throws
            org.apache.accumulo.proxy.thrift.AccumuloException,
            org.apache.accumulo.proxy.thrift.AccumuloSecurityException,
            org.apache.accumulo.proxy.thrift.TableNotFoundException, TException {
        try {
            org.apache.accumulo.core.client.IteratorSetting is = getConnector(login).tableOperations().getIteratorSetting(tableName, iteratorName, getIteratorScope(scope));
            return new org.apache.accumulo.proxy.thrift.IteratorSetting(is.getPriority(), is.getName(), is.getIteratorClass(), is.getOptions());
        } catch (AccumuloException e) {
            throw new org.apache.accumulo.proxy.thrift.AccumuloException(e.toString());
        } catch(AccumuloSecurityException e)  {
            throw new org.apache.accumulo.proxy.thrift.AccumuloSecurityException(e.toString());
        } catch(TableNotFoundException e) {
            throw new org.apache.accumulo.proxy.thrift.TableNotFoundException(e.toString());
        } catch (Exception e) {
            throw new TException(e.toString());
        }
    }

    @Override
    public ByteBuffer getMaxRow(ByteBuffer login, String tableName, Set<ByteBuffer> auths, ByteBuffer startRow, boolean startInclusive, ByteBuffer endRow, boolean endInclusive) throws
            org.apache.accumulo.proxy.thrift.AccumuloException,
            org.apache.accumulo.proxy.thrift.AccumuloSecurityException,
            org.apache.accumulo.proxy.thrift.TableNotFoundException, TException {
        try {
            Connector connector = getConnector(login);
            Text startText = ByteBufferUtil.toText(startRow);
            Text endText = ByteBufferUtil.toText(endRow);
            Authorizations auth;
            if (auths != null) {
                auth = getAuthorizations(auths);
            } else {
                auth = connector.securityOperations().getUserAuthorizations(connector.whoami());
            }
            Text max = connector.tableOperations().getMaxRow(tableName, auth, startText, startInclusive, endText, endInclusive);
            return TextUtil.getByteBuffer(max);
        } catch (AccumuloException e) {
            throw new org.apache.accumulo.proxy.thrift.AccumuloException(e.toString());
        } catch(AccumuloSecurityException e)  {
            throw new org.apache.accumulo.proxy.thrift.AccumuloSecurityException(e.toString());
        } catch(TableNotFoundException e) {
            throw new org.apache.accumulo.proxy.thrift.TableNotFoundException(e.toString());
        } catch (Exception e) {
            throw new TException(e.toString());
        }
    }

    @Override
    public Map<String, String> getTableProperties(ByteBuffer login, String tableName) throws
            org.apache.accumulo.proxy.thrift.AccumuloException,
            org.apache.accumulo.proxy.thrift.TableNotFoundException, TException {
        try {
            Map<String,String> ret = new HashMap<String,String>();

            for (Map.Entry<String,String> entry : getConnector(login).tableOperations().getProperties(tableName)) {
                ret.put(entry.getKey(), entry.getValue());
            }
            return ret;
        } catch (AccumuloException e) {
            throw new org.apache.accumulo.proxy.thrift.AccumuloException(e.toString());
        } catch(AccumuloSecurityException e)  {
            throw new org.apache.accumulo.proxy.thrift.AccumuloException(e.toString());  // TODO: Should this throw securityException?
        } catch(TableNotFoundException e) {
            throw new org.apache.accumulo.proxy.thrift.TableNotFoundException(e.toString());
        } catch (Exception e) {
            throw new TException(e.toString());
        }
    }

    @Override
    public void importDirectory(ByteBuffer login, String tableName, String importDir, String failureDir, boolean setTime) throws
            org.apache.accumulo.proxy.thrift.TableNotFoundException,
            org.apache.accumulo.proxy.thrift.AccumuloException,
            org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {
        try {
            getConnector(login).tableOperations().importDirectory(tableName, importDir, failureDir, setTime);
        } catch (AccumuloException e) {
            throw new org.apache.accumulo.proxy.thrift.AccumuloException(e.toString());
        } catch(AccumuloSecurityException e)  {
            throw new org.apache.accumulo.proxy.thrift.AccumuloSecurityException(e.toString());
        } catch(TableNotFoundException e) {
            throw new org.apache.accumulo.proxy.thrift.TableNotFoundException(e.toString());
        } catch (Exception e) {
            throw new TException(e.toString());
        }
    }
    @Override
    public List<ByteBuffer> listSplits(ByteBuffer login, String tableName, int maxSplits) throws
            org.apache.accumulo.proxy.thrift.AccumuloException,
            org.apache.accumulo.proxy.thrift.AccumuloSecurityException,
            org.apache.accumulo.proxy.thrift.TableNotFoundException, TException {
        try {
            Collection<Text> splits = getConnector(login).tableOperations().getSplits(tableName, maxSplits);
            List<ByteBuffer> ret = new ArrayList<ByteBuffer>();
            for (Text split : splits) {
                ret.add(TextUtil.getByteBuffer(split));
            }
            return ret;
        } catch (AccumuloException e) {
            throw new org.apache.accumulo.proxy.thrift.AccumuloException(e.toString());
        } catch(AccumuloSecurityException e)  {
            throw new org.apache.accumulo.proxy.thrift.AccumuloSecurityException(e.toString());
        } catch(TableNotFoundException e) {
            throw new org.apache.accumulo.proxy.thrift.TableNotFoundException(e.toString());
        } catch (Exception e) {
            throw new TException(e.toString());
        }
    }

    @Override
    public Set<String> listTables(ByteBuffer login) throws TException {
        try {
            return getConnector(login).tableOperations().list();
        } catch (Exception e) {
            throw new TException(e.toString());
        }
    }

    @Override
    public Map<String, Set<IteratorScope>> listIterators(ByteBuffer login, String tableName) throws
            org.apache.accumulo.proxy.thrift.AccumuloException,
            org.apache.accumulo.proxy.thrift.AccumuloSecurityException,
            org.apache.accumulo.proxy.thrift.TableNotFoundException, TException {

        try {
            Map<String,EnumSet<IteratorUtil.IteratorScope>> iterMap = getConnector(login).tableOperations().listIterators(tableName);
            Map<String,Set<org.apache.accumulo.proxy.thrift.IteratorScope>> result = new HashMap<String,Set<org.apache.accumulo.proxy.thrift.IteratorScope>>();
            for (Map.Entry<String,EnumSet<IteratorUtil.IteratorScope>> entry : iterMap.entrySet()) {
                result.put(entry.getKey(), getProxyIteratorScopes(entry.getValue()));
            }
            return result;
        } catch (AccumuloException e) {
            throw new org.apache.accumulo.proxy.thrift.AccumuloException(e.toString());
        } catch(AccumuloSecurityException e)  {
            throw new org.apache.accumulo.proxy.thrift.AccumuloSecurityException(e.toString());
        } catch(TableNotFoundException e) {
            throw new org.apache.accumulo.proxy.thrift.TableNotFoundException(e.toString());
        } catch (Exception e) {
            throw new TException(e.toString());
        }
    }

    @Override
    public Map<String, Integer> listConstraints(ByteBuffer login, String tableName) throws
            org.apache.accumulo.proxy.thrift.AccumuloException,
            org.apache.accumulo.proxy.thrift.TableNotFoundException, TException {

        try {

            Connector connector = getConnector(login);

            Map<String,Integer> constraints = new TreeMap<String,Integer>();
            for (Map.Entry<String,String> property : connector.tableOperations().getProperties(tableName)) {
                if (property.getKey().startsWith(Property.TABLE_CONSTRAINT_PREFIX.toString())) {
                    if (constraints.containsKey(property.getValue()))
                        throw new AccumuloException("Same constraint configured twice: " + property.getKey() + "=" + Property.TABLE_CONSTRAINT_PREFIX
                                + constraints.get(property.getValue()) + "=" + property.getKey());
                    try {
                        constraints.put(property.getValue(), Integer.parseInt(property.getKey().substring(Property.TABLE_CONSTRAINT_PREFIX.toString().length())));
                    } catch (NumberFormatException e) {
                        throw new AccumuloException("Bad key for existing constraint: " + property.toString());
                    }
                }
            }
            return constraints;
        } catch (AccumuloSecurityException e) {
            throw new org.apache.accumulo.proxy.thrift.AccumuloException(e.toString()); // TODO: should this throw securityException?
        } catch (AccumuloException e) {
            throw new org.apache.accumulo.proxy.thrift.AccumuloException(e.toString());
        } catch (TableNotFoundException e) {
            throw new org.apache.accumulo.proxy.thrift.TableNotFoundException(e.toString());
        } catch (Exception e) {
            throw new TException(e.toString());
        }
    }

    @Override
    public void mergeTablets(ByteBuffer login, String tableName, ByteBuffer startRow, ByteBuffer endRow) throws
            org.apache.accumulo.proxy.thrift.AccumuloException,
            org.apache.accumulo.proxy.thrift.AccumuloSecurityException,
            org.apache.accumulo.proxy.thrift.TableNotFoundException, TException {
        try {
            getConnector(login).tableOperations().merge(tableName, ByteBufferUtil.toText(startRow), ByteBufferUtil.toText(endRow));
        } catch (AccumuloException e) {
            throw new org.apache.accumulo.proxy.thrift.AccumuloException(e.toString());
        } catch(AccumuloSecurityException e)  {
            throw new org.apache.accumulo.proxy.thrift.AccumuloSecurityException(e.toString());
        } catch(TableNotFoundException e) {
            throw new org.apache.accumulo.proxy.thrift.TableNotFoundException(e.toString());
        } catch (Exception e) {
            throw new TException(e.toString());
        }
    }

    @Override
    public void offlineTable(ByteBuffer login, String tableName) throws
            org.apache.accumulo.proxy.thrift.AccumuloException,
            org.apache.accumulo.proxy.thrift.AccumuloSecurityException,
            org.apache.accumulo.proxy.thrift.TableNotFoundException, TException {
        try {
            getConnector(login).tableOperations().offline(tableName);
        } catch (AccumuloException e) {
            throw new org.apache.accumulo.proxy.thrift.AccumuloException(e.toString());
        } catch(AccumuloSecurityException e)  {
            throw new org.apache.accumulo.proxy.thrift.AccumuloSecurityException(e.toString());
        } catch(TableNotFoundException e) {
            throw new org.apache.accumulo.proxy.thrift.TableNotFoundException(e.toString());
        } catch (Exception e) {
            throw new TException(e.toString());
        }
    }

    @Override
    public void onlineTable(ByteBuffer login, String tableName) throws
            org.apache.accumulo.proxy.thrift.AccumuloException,
            org.apache.accumulo.proxy.thrift.AccumuloSecurityException,
            org.apache.accumulo.proxy.thrift.TableNotFoundException, TException {
        try {
            getConnector(login).tableOperations().online(tableName);
        } catch (AccumuloException e) {
            throw new org.apache.accumulo.proxy.thrift.AccumuloException(e.toString());
        } catch(AccumuloSecurityException e)  {
            throw new org.apache.accumulo.proxy.thrift.AccumuloSecurityException(e.toString());
        } catch(TableNotFoundException e) {
            throw new org.apache.accumulo.proxy.thrift.TableNotFoundException(e.toString());
        } catch (Exception e) {
            throw new TException(e.toString());
        }
    }

    @Override
    public void removeConstraint(ByteBuffer login, String tableName, int constraint) throws
            org.apache.accumulo.proxy.thrift.AccumuloException,
            org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {

        try {
            getConnector(login).tableOperations().removeProperty(tableName,
                    Property.TABLE_CONSTRAINT_PREFIX.toString() + constraint);
        } catch (AccumuloSecurityException e) {
            throw new org.apache.accumulo.proxy.thrift.AccumuloSecurityException(e.toString());
        } catch (AccumuloException e) {
            throw new org.apache.accumulo.proxy.thrift.AccumuloException(e.toString());
        } catch (Exception e) {
            throw new TException(e.toString());
        }
    }

    @Override
    public void removeIterator(ByteBuffer login, String tableName, String iterName, Set<IteratorScope> scopes) throws
            org.apache.accumulo.proxy.thrift.AccumuloException,
            org.apache.accumulo.proxy.thrift.AccumuloSecurityException,
            org.apache.accumulo.proxy.thrift.TableNotFoundException, TException {

        try {
            getConnector(login).tableOperations().removeIterator(tableName, iterName, getIteratorScopes(scopes));
        } catch (AccumuloException e) {
            throw new org.apache.accumulo.proxy.thrift.AccumuloException(e.toString());
        } catch(AccumuloSecurityException e)  {
            throw new org.apache.accumulo.proxy.thrift.AccumuloSecurityException(e.toString());
        } catch(TableNotFoundException e) {
            throw new org.apache.accumulo.proxy.thrift.TableNotFoundException(e.toString());
        } catch (Exception e) {
            throw new TException(e.toString());
        }
    }

    @Override
    public void removeTableProperty(ByteBuffer login, String tableName, String property) throws
            org.apache.accumulo.proxy.thrift.AccumuloException,
            org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {
        try {
            getConnector(login).tableOperations().removeProperty(tableName, property);
        } catch (AccumuloException e) {
            throw new org.apache.accumulo.proxy.thrift.AccumuloException(e.toString());
        } catch(AccumuloSecurityException e)  {
            throw new org.apache.accumulo.proxy.thrift.AccumuloSecurityException(e.toString());
        } catch (Exception e) {
            throw new TException(e.toString());
        }
    }

    @Override
    public void renameTable(ByteBuffer login, String oldTableName, String newTableName) throws
            org.apache.accumulo.proxy.thrift.AccumuloException,
            org.apache.accumulo.proxy.thrift.AccumuloSecurityException,
            org.apache.accumulo.proxy.thrift.TableNotFoundException,
            org.apache.accumulo.proxy.thrift.TableExistsException, TException {
        try {
            getConnector(login).tableOperations().rename(oldTableName, newTableName);
        } catch (AccumuloException e) {
            throw new org.apache.accumulo.proxy.thrift.AccumuloException(e.toString());
        } catch(AccumuloSecurityException e)  {
            throw new org.apache.accumulo.proxy.thrift.AccumuloSecurityException(e.toString());
        } catch(TableNotFoundException e) {
            throw new org.apache.accumulo.proxy.thrift.TableNotFoundException(e.toString());
        } catch(TableExistsException e) {
            throw new org.apache.accumulo.proxy.thrift.TableExistsException(e.toString());
        } catch (Exception e) {
            throw new TException(e.toString());
        }
    }

    @Override
    public void setLocalityGroups(ByteBuffer login, String tableName, Map<String, Set<String>> groupStrings) throws
            org.apache.accumulo.proxy.thrift.AccumuloException,
            org.apache.accumulo.proxy.thrift.AccumuloSecurityException,
            org.apache.accumulo.proxy.thrift.TableNotFoundException, TException {
        try {
            Map<String,Set<Text>> groups = new HashMap<String,Set<Text>>();
            for (String key : groupStrings.keySet()) {
                groups.put(key, new HashSet<Text>());
                for (String val : groupStrings.get(key)) {
                    groups.get(key).add(new Text(val));
                }
            }
            getConnector(login).tableOperations().setLocalityGroups(tableName, groups);
        } catch (AccumuloException e) {
            throw new org.apache.accumulo.proxy.thrift.AccumuloException(e.toString());
        } catch(AccumuloSecurityException e)  {
            throw new org.apache.accumulo.proxy.thrift.AccumuloSecurityException(e.toString());
        } catch(TableNotFoundException e) {
            throw new org.apache.accumulo.proxy.thrift.TableNotFoundException(e.toString());
        } catch (Exception e) {
            throw new TException(e.toString());
        }
    }

    @Override
    public void setTableProperty(ByteBuffer login, String tableName, String property, String value) throws
            org.apache.accumulo.proxy.thrift.AccumuloException,
            org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {
        try {
            getConnector(login).tableOperations().setProperty(tableName, property, value);
        } catch (AccumuloException e) {
            throw new org.apache.accumulo.proxy.thrift.AccumuloException(e.toString());
        } catch(AccumuloSecurityException e)  {
            throw new org.apache.accumulo.proxy.thrift.AccumuloSecurityException(e.toString());
        } catch (Exception e) {
            throw new TException(e.toString());
        }
    }

    @Override
    public Set<Range> splitRangeByTablets(ByteBuffer login, String tableName, Range range, int maxSplits) throws
            org.apache.accumulo.proxy.thrift.AccumuloException,
            org.apache.accumulo.proxy.thrift.AccumuloSecurityException,
            org.apache.accumulo.proxy.thrift.TableNotFoundException, TException {
        try {
            Set<org.apache.accumulo.core.data.Range> ranges = getConnector(login).tableOperations().splitRangeByTablets(tableName, getRange(range), maxSplits);
            Set<org.apache.accumulo.proxy.thrift.Range> result = new HashSet<org.apache.accumulo.proxy.thrift.Range>();
            for (org.apache.accumulo.core.data.Range r : ranges) {
                result.add(getRange(r));
            }
            return result;
        } catch (AccumuloException e) {
            throw new org.apache.accumulo.proxy.thrift.AccumuloException(e.toString());
        } catch(AccumuloSecurityException e)  {
            throw new org.apache.accumulo.proxy.thrift.AccumuloSecurityException(e.toString());
        } catch(TableNotFoundException e) {
            throw new org.apache.accumulo.proxy.thrift.TableNotFoundException(e.toString());
        } catch (Exception e) {
            throw new TException(e.toString());
        }
    }

    @Override
    public boolean tableExists(ByteBuffer login, String tableName) throws TException {
        try {
            return getConnector(login).tableOperations().exists(tableName);
        } catch (Exception e) {
            throw new TException(e.getMessage());
        }
    }

    @Override
    public Map<String, String> tableIdMap(ByteBuffer login) throws TException {
        try {
            return getConnector(login).tableOperations().tableIdMap();
        } catch (Exception e) {
            throw new TException(e.getMessage());
        }
    }


    @Override
    public List<ActiveScan> getActiveScans(ByteBuffer login, String tserver) throws
            org.apache.accumulo.proxy.thrift.AccumuloException,
            org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {
        List<org.apache.accumulo.proxy.thrift.ActiveScan> result = new ArrayList<org.apache.accumulo.proxy.thrift.ActiveScan>();
        try {
            List<org.apache.accumulo.core.client.admin.ActiveScan> activeScans = getConnector(login).instanceOperations().getActiveScans(tserver);
            for (org.apache.accumulo.core.client.admin.ActiveScan scan : activeScans) {
                org.apache.accumulo.proxy.thrift.ActiveScan pscan = new org.apache.accumulo.proxy.thrift.ActiveScan();
                pscan.client = scan.getClient();
                pscan.user = scan.getUser();
                pscan.table = scan.getTable();
                pscan.age = scan.getAge();
                pscan.type = ScanType.valueOf(scan.getType().toString());
                pscan.state = ScanState.valueOf(scan.getState().toString());
                org.apache.accumulo.core.data.KeyExtent e = scan.getExtent();
                pscan.extent = new org.apache.accumulo.proxy.thrift.KeyExtent(e.getTableId().toString(), TextUtil.getByteBuffer(e.getEndRow()),
                        TextUtil.getByteBuffer(e.getPrevEndRow()));
                pscan.columns = new ArrayList<org.apache.accumulo.proxy.thrift.Column>();
                if (scan.getColumns() != null) {
                    for (org.apache.accumulo.core.data.Column c : scan.getColumns()) {
                        org.apache.accumulo.proxy.thrift.Column column = new org.apache.accumulo.proxy.thrift.Column();
                        column.setColFamily(c.getColumnFamily());
                        column.setColQualifier(c.getColumnQualifier());
                        column.setColVisibility(c.getColumnVisibility());
                        pscan.columns.add(column);
                    }
                }
                pscan.iterators = new ArrayList<org.apache.accumulo.proxy.thrift.IteratorSetting>();
                for (String iteratorString : scan.getSsiList()) {
                    String[] parts = iteratorString.split("[=,]");
                    if (parts.length == 3) {
                        String name = parts[0];
                        int priority = Integer.parseInt(parts[1]);
                        String classname = parts[2];
                        org.apache.accumulo.proxy.thrift.IteratorSetting settings = new org.apache.accumulo.proxy.thrift.IteratorSetting(priority, name, classname, scan
                                .getSsio().get(name));
                        pscan.iterators.add(settings);
                    }
                }
                result.add(pscan);
            }
            return result;
        } catch(AccumuloSecurityException e)  {
            throw new org.apache.accumulo.proxy.thrift.AccumuloSecurityException(e.toString());
        } catch(AccumuloException e) {
            throw new org.apache.accumulo.proxy.thrift.AccumuloException(e.toString());
        } catch (Exception e) {
            throw new TException(e.toString());
        }
    }


    @Override
    public Map<String, String> getSiteConfiguration(ByteBuffer login) throws
            org.apache.accumulo.proxy.thrift.AccumuloException,
            org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {
        try {
            return getConnector(login).instanceOperations().getSiteConfiguration();
        } catch (AccumuloException e) {
            throw new org.apache.accumulo.proxy.thrift.AccumuloException(e.toString());
        } catch(AccumuloSecurityException e)  {
            throw new org.apache.accumulo.proxy.thrift.AccumuloSecurityException(e.toString());
        } catch (Exception e) {
            throw new TException(e.toString());
        }
    }

    @Override
    public Map<String, String> getSystemConfiguration(ByteBuffer login) throws
            org.apache.accumulo.proxy.thrift.AccumuloException,
            org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {
        try {
            return getConnector(login).instanceOperations().getSystemConfiguration();
        } catch (AccumuloException e) {
            throw new org.apache.accumulo.proxy.thrift.AccumuloException(e.toString());
        } catch(AccumuloSecurityException e)  {
            throw new org.apache.accumulo.proxy.thrift.AccumuloSecurityException(e.toString());
        } catch (Exception e) {
            throw new TException(e.toString());
        }
    }

    @Override
    public List<String> getTabletServers(ByteBuffer login) throws TException {
        try {
            return getConnector(login).instanceOperations().getTabletServers();
        } catch (Exception e) {
            throw new TException(e.getMessage());
        }
    }

    @Override
    public void removeProperty(ByteBuffer login, String property) throws
            org.apache.accumulo.proxy.thrift.AccumuloException,
            org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {

        try {
            getConnector(login).instanceOperations().removeProperty(property);
        } catch (AccumuloException e) {
            throw new org.apache.accumulo.proxy.thrift.AccumuloException(e.toString());
        } catch(AccumuloSecurityException e)  {
            throw new org.apache.accumulo.proxy.thrift.AccumuloSecurityException(e.toString());
        } catch (Exception e) {
            throw new TException(e.toString());
        }
    }

    @Override
    public void setProperty(ByteBuffer login, String property, String value) throws
            org.apache.accumulo.proxy.thrift.AccumuloException,
            org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {
        try {
            getConnector(login).instanceOperations().setProperty(property, value);
        } catch (AccumuloException e) {
            throw new org.apache.accumulo.proxy.thrift.AccumuloException(e.toString());
        } catch(AccumuloSecurityException e)  {
            throw new org.apache.accumulo.proxy.thrift.AccumuloSecurityException(e.toString());
        } catch (Exception e) {
            throw new TException(e.toString());
        }
    }

    @Override
    public boolean testClassLoad(ByteBuffer login, String className, String asTypeName) throws
            org.apache.accumulo.proxy.thrift.AccumuloException,
            org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {
        try {
            return getConnector(login).instanceOperations().testClassLoad(className, asTypeName);
        } catch (AccumuloException e) {
            throw new org.apache.accumulo.proxy.thrift.AccumuloException(e.toString());
        } catch(AccumuloSecurityException e)  {
            throw new org.apache.accumulo.proxy.thrift.AccumuloSecurityException(e.toString());
        } catch (Exception e) {

            return false;
        }
    }

    @Override
    public boolean authenticateUser(ByteBuffer login, String user, Map<String, String> properties) throws
            org.apache.accumulo.proxy.thrift.AccumuloException,
            org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {
        try {
            return getConnector(login).securityOperations().authenticateUser(user, properties.get(PASSWORD_PROP).getBytes());
        } catch (AccumuloException e) {
            throw new org.apache.accumulo.proxy.thrift.AccumuloException(e.toString());
        } catch(AccumuloSecurityException e)  {
            throw new org.apache.accumulo.proxy.thrift.AccumuloSecurityException(e.toString());
        } catch (Exception e) {
            throw new TException(e.toString());
        }
    }

    @Override
    public void changeUserAuthorizations(ByteBuffer login, String user, Set<ByteBuffer> authorizations) throws
            org.apache.accumulo.proxy.thrift.AccumuloException,
            org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {
        try {
            Set<String> auths = new HashSet<String>();
            for (ByteBuffer auth : authorizations) {
                auths.add(ByteBufferUtil.toString(auth));
            }
            getConnector(login).securityOperations().changeUserAuthorizations(user, new Authorizations(auths.toArray(new String[0])));
        } catch (AccumuloException e) {
            throw new org.apache.accumulo.proxy.thrift.AccumuloException(e.toString());
        } catch(AccumuloSecurityException e)  {
            throw new org.apache.accumulo.proxy.thrift.AccumuloSecurityException(e.toString());
        } catch (Exception e) {
            throw new TException(e.toString());
        }
    }

    @Override
    public void changeLocalUserPassword(ByteBuffer login, String user, ByteBuffer password) throws
            org.apache.accumulo.proxy.thrift.AccumuloException,
            org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {
        try {
            getConnector(login).securityOperations().changeUserPassword(user, password.array());
        } catch (AccumuloException e) {
            throw new org.apache.accumulo.proxy.thrift.AccumuloException(e.toString());
        } catch(AccumuloSecurityException e)  {
            throw new org.apache.accumulo.proxy.thrift.AccumuloSecurityException(e.toString());
        } catch (Exception e) {
            throw new TException(e.toString());
        }
    }

    @Override
    public void createLocalUser(ByteBuffer login, String user, ByteBuffer password) throws
            org.apache.accumulo.proxy.thrift.AccumuloException,
            org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {
        try {
            getConnector(login).securityOperations().createUser(user, password.array(), new Authorizations());
        } catch (AccumuloException e) {
            throw new org.apache.accumulo.proxy.thrift.AccumuloException(e.toString());
        } catch(AccumuloSecurityException e)  {
            throw new org.apache.accumulo.proxy.thrift.AccumuloSecurityException(e.toString());
        } catch (Exception e) {
            throw new TException(e.toString());
        }
    }

    @Override
    public void dropLocalUser(ByteBuffer login, String user) throws
            org.apache.accumulo.proxy.thrift.AccumuloException,
            org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {

        try {
            getConnector(login).securityOperations().dropUser(user);
        } catch (AccumuloException e) {
            throw new org.apache.accumulo.proxy.thrift.AccumuloException(e.toString());
        } catch(AccumuloSecurityException e)  {
            throw new org.apache.accumulo.proxy.thrift.AccumuloSecurityException(e.toString());
        } catch (Exception e) {
            throw new TException(e.toString());
        }
    }

    @Override
    public List<ByteBuffer> getUserAuthorizations(ByteBuffer login, String user) throws
            org.apache.accumulo.proxy.thrift.AccumuloException,
            org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {
        try {
            return getConnector(login).securityOperations().getUserAuthorizations(user).getAuthorizationsBB();
        } catch (AccumuloException e) {
            throw new org.apache.accumulo.proxy.thrift.AccumuloException(e.toString());
        } catch(AccumuloSecurityException e)  {
            throw new org.apache.accumulo.proxy.thrift.AccumuloSecurityException(e.toString());
        } catch (Exception e) {
            throw new TException(e.toString());
        }
    }

    @Override
    public void grantSystemPermission(ByteBuffer login, String user, SystemPermission perm) throws
            org.apache.accumulo.proxy.thrift.AccumuloException,
            org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {
        try {
            getConnector(login).securityOperations().grantSystemPermission(user, org.apache.accumulo.core.security.SystemPermission.getPermissionById((byte) perm.getValue()));
        } catch (AccumuloException e) {
            throw new org.apache.accumulo.proxy.thrift.AccumuloException(e.toString());
        } catch(AccumuloSecurityException e)  {
            throw new org.apache.accumulo.proxy.thrift.AccumuloSecurityException(e.toString());
        } catch (Exception e) {
            throw new TException(e.toString());
        }
    }

    @Override
    public void grantTablePermission(ByteBuffer login, String user, String table, TablePermission perm) throws
            org.apache.accumulo.proxy.thrift.AccumuloException,
            org.apache.accumulo.proxy.thrift.AccumuloSecurityException,
            org.apache.accumulo.proxy.thrift.TableNotFoundException, TException {  // TODO: TableNotFound was never thrown
        try {
            getConnector(login).securityOperations().grantTablePermission(user, table, org.apache.accumulo.core.security.TablePermission.getPermissionById((byte) perm.getValue()));
        } catch (AccumuloException e) {
            throw new org.apache.accumulo.proxy.thrift.AccumuloException(e.toString());
        } catch(AccumuloSecurityException e)  {
            throw new org.apache.accumulo.proxy.thrift.AccumuloSecurityException(e.toString());
        } catch (Exception e) {
            throw new TException(e.toString());
        }
    }

    @Override
    public boolean hasSystemPermission(ByteBuffer login, String user, SystemPermission perm) throws
            org.apache.accumulo.proxy.thrift.AccumuloException,
            org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {
        try {
            return getConnector(login).securityOperations().hasSystemPermission(user, org.apache.accumulo.core.security.SystemPermission.getPermissionById((byte) perm.getValue()));
        } catch (AccumuloException e) {
            throw new org.apache.accumulo.proxy.thrift.AccumuloException(e.toString());
        } catch(AccumuloSecurityException e)  {
            throw new org.apache.accumulo.proxy.thrift.AccumuloSecurityException(e.toString());
        } catch (Exception e) {
            throw new TException(e.toString());
        }
    }

    @Override
    public boolean hasTablePermission(ByteBuffer login, String user, String table, TablePermission perm) throws
            org.apache.accumulo.proxy.thrift.AccumuloException,
            org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {
        try {
            return getConnector(login).securityOperations().hasTablePermission(user, table, org.apache.accumulo.core.security.TablePermission.getPermissionById((byte) perm.getValue()));
        } catch (AccumuloException e) {
            throw new org.apache.accumulo.proxy.thrift.AccumuloException(e.toString());
        } catch(AccumuloSecurityException e)  {
            throw new org.apache.accumulo.proxy.thrift.AccumuloSecurityException(e.toString());
        } catch (Exception e) {
            throw new TException(e.toString());
        }
    }

    @Override
    public Set<String> listLocalUsers(ByteBuffer login) throws
            org.apache.accumulo.proxy.thrift.AccumuloException,
            org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {

        try {
            return getConnector(login).securityOperations().listUsers();
        } catch (AccumuloException e) {
            throw new org.apache.accumulo.proxy.thrift.AccumuloException(e.toString());
        } catch(AccumuloSecurityException e)  {
            throw new org.apache.accumulo.proxy.thrift.AccumuloSecurityException(e.toString());
        } catch (Exception e) {
            throw new TException(e.toString());
        }
    }

    @Override
    public void revokeSystemPermission(ByteBuffer login, String user, SystemPermission perm) throws
            org.apache.accumulo.proxy.thrift.AccumuloException,
            org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {
        try {
            getConnector(login).securityOperations().revokeSystemPermission(user, org.apache.accumulo.core.security.SystemPermission.getPermissionById((byte) perm.getValue()));
        } catch (AccumuloException e) {
            throw new org.apache.accumulo.proxy.thrift.AccumuloException(e.toString());
        } catch(AccumuloSecurityException e)  {
            throw new org.apache.accumulo.proxy.thrift.AccumuloSecurityException(e.toString());
        } catch (Exception e) {
            throw new TException(e.toString());
        }
    }

    @Override
    public void revokeTablePermission(ByteBuffer login, String user, String table, TablePermission perm) throws
            org.apache.accumulo.proxy.thrift.AccumuloException,
            org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {
        try {
            getConnector(login).securityOperations().revokeTablePermission(user, table, org.apache.accumulo.core.security.TablePermission.getPermissionById((byte) perm.getValue()));
        } catch (AccumuloException e) {
            throw new org.apache.accumulo.proxy.thrift.AccumuloException(e.toString());
        } catch(AccumuloSecurityException e)  {
            throw new org.apache.accumulo.proxy.thrift.AccumuloSecurityException(e.toString());
        } catch (Exception e) {
            throw new TException(e.toString());
        }
    }

    @Override
    public String createBatchScanner(ByteBuffer login, String tableName, BatchScanOptions opts) throws
            org.apache.accumulo.proxy.thrift.AccumuloException,
            org.apache.accumulo.proxy.thrift.AccumuloSecurityException,
            org.apache.accumulo.proxy.thrift.TableNotFoundException, TException {
        try {
            Connector connector = getConnector(login);

            int threads = 10;
            Authorizations auth;
            if (opts != null && opts.isSetAuthorizations()) {
                auth = getAuthorizations(opts.authorizations);
            } else {
                auth = connector.securityOperations().getUserAuthorizations(connector.whoami());
            }
            if (opts != null && opts.threads > 0)
                threads = opts.threads;

            BatchScanner scanner = connector.createBatchScanner(tableName, auth, threads);

            if (opts != null) {
                if (opts.iterators != null) {
                    for (org.apache.accumulo.proxy.thrift.IteratorSetting iter : opts.iterators) {
                        org.apache.accumulo.core.client.IteratorSetting is = new org.apache.accumulo.core.client.IteratorSetting(iter.getPriority(), iter.getName(), iter.getIteratorClass(), iter.getProperties());
                        scanner.addScanIterator(is);
                    }
                }

                ArrayList<org.apache.accumulo.core.data.Range> ranges = new ArrayList<org.apache.accumulo.core.data.Range>();

                if (opts.ranges == null) {
                    ranges.add(new org.apache.accumulo.core.data.Range());
                } else {
                    for (org.apache.accumulo.proxy.thrift.Range range : opts.ranges) {
                        org.apache.accumulo.core.data.Range aRange = new org.apache.accumulo.core.data.Range(range.getStart() == null ? null : Util.fromThrift(range.getStart()), true, range.getStop() == null ? null
                                : Util.fromThrift(range.getStop()), false);
                        ranges.add(aRange);
                    }
                }
                scanner.setRanges(ranges);

                if (opts.columns != null) {
                    for (ScanColumn col : opts.columns) {
                        if (col.isSetColQualifier())
                            scanner.fetchColumn(ByteBufferUtil.toText(col.colFamily), ByteBufferUtil.toText(col.colQualifier));
                        else
                            scanner.fetchColumnFamily(ByteBufferUtil.toText(col.colFamily));
                    }
                }
            }

            UUID uuid = UUID.randomUUID();

            ScannerPlusIterator spi = new ScannerPlusIterator();
            spi.scanner = scanner;
            spi.iterator = scanner.iterator();
            scannerCache.put(uuid, spi);
            return uuid.toString();
        } catch (AccumuloException e) {
            throw new org.apache.accumulo.proxy.thrift.AccumuloException(e.toString());
        } catch(AccumuloSecurityException e)  {
            throw new org.apache.accumulo.proxy.thrift.AccumuloSecurityException(e.toString());
        } catch (TableNotFoundException e) {
            throw new org.apache.accumulo.proxy.thrift.TableNotFoundException(e.toString());
        } catch (Exception e) {
            throw new TException(e.toString());
        }
    }

    @Override
    public String createScanner(ByteBuffer login, String tableName, ScanOptions opts) throws
            org.apache.accumulo.proxy.thrift.AccumuloException,
            org.apache.accumulo.proxy.thrift.AccumuloSecurityException,
            org.apache.accumulo.proxy.thrift.TableNotFoundException, TException {
        try {
            Connector connector = getConnector(login);

            Authorizations auth;
            if (opts != null && opts.isSetAuthorizations()) {
                auth = getAuthorizations(opts.authorizations);
            } else {
                auth = connector.securityOperations().getUserAuthorizations(connector.whoami());
            }
            Scanner scanner = connector.createScanner(tableName, auth);

            if (opts != null) {
                if (opts.iterators != null) {
                    for (org.apache.accumulo.proxy.thrift.IteratorSetting iter : opts.iterators) {
                        org.apache.accumulo.core.client.IteratorSetting is = new org.apache.accumulo.core.client.IteratorSetting(iter.getPriority(), iter.getName(), iter.getIteratorClass(), iter.getProperties());
                        scanner.addScanIterator(is);
                    }
                }
                org.apache.accumulo.proxy.thrift.Range prange = opts.range;
                if (prange != null) {
                    org.apache.accumulo.core.data.Range range = new org.apache.accumulo.core.data.Range(Util.fromThrift(prange.getStart()), prange.startInclusive, Util.fromThrift(prange.getStop()), prange.stopInclusive);
                    scanner.setRange(range);
                }
                if (opts.columns != null) {
                    for (ScanColumn col : opts.columns) {
                        if (col.isSetColQualifier())
                            scanner.fetchColumn(ByteBufferUtil.toText(col.colFamily), ByteBufferUtil.toText(col.colQualifier));
                        else
                            scanner.fetchColumnFamily(ByteBufferUtil.toText(col.colFamily));
                    }
                }
            }

            UUID uuid = UUID.randomUUID();

            ScannerPlusIterator spi = new ScannerPlusIterator();
            spi.scanner = scanner;
            spi.iterator = scanner.iterator();
            scannerCache.put(uuid, spi);
            return uuid.toString();
        } catch (AccumuloException e) {
            throw new org.apache.accumulo.proxy.thrift.AccumuloException(e.toString());
        } catch(AccumuloSecurityException e)  {
            throw new org.apache.accumulo.proxy.thrift.AccumuloSecurityException(e.toString());
        } catch (TableNotFoundException e) {
            throw new org.apache.accumulo.proxy.thrift.TableNotFoundException(e.toString());
        } catch (Exception e) {
            throw new TException(e.toString());
        }
    }

    @Override
    public boolean hasNext(String scanner) throws UnknownScanner, TException {
        ScannerPlusIterator spi = scannerCache.getIfPresent(UUID.fromString(scanner));
        if (spi == null) {
            throw new UnknownScanner("Scanner never existed or no longer exists");
        }

        return (spi.iterator.hasNext());
    }

    @Override
    public KeyValueAndPeek nextEntry(String scanner) throws NoMoreEntriesException, UnknownScanner,
            org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {

        ScanResult scanResult = nextK(scanner, 1);
        if (scanResult.results.size() > 0) {
            return new KeyValueAndPeek(scanResult.results.get(0), scanResult.isMore());
        } else {
            throw new NoMoreEntriesException();
        }
    }

    @Override
    public ScanResult nextK(String scanner, int k) throws NoMoreEntriesException, UnknownScanner,
            org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {

        // fetch the scanner
        ScannerPlusIterator spi = scannerCache.getIfPresent(UUID.fromString(scanner));
        if (spi == null) {
            throw new UnknownScanner("Scanner never existed or no longer exists");
        }
        Iterator<Map.Entry<org.apache.accumulo.core.data.Key,Value>> batchScanner = spi.iterator;
        // synchronized to prevent race conditions
        synchronized (batchScanner) {
            ScanResult ret = new ScanResult();
            ret.setResults(new ArrayList<KeyValue>());
            int numRead = 0;
            try {
                while (batchScanner.hasNext() && numRead < k) {
                    Map.Entry<org.apache.accumulo.core.data.Key,Value> next = batchScanner.next();
                    ret.addToResults(new KeyValue(Util.toThrift(next.getKey()), ByteBuffer.wrap(next.getValue().get())));
                    numRead++;
                }
                ret.setMore(numRead == k);
            } catch (Exception ex) {
                closeScanner(scanner);
                throw new TException(ex);
            }
            return ret;
        }
    }

    @Override
    public void closeScanner(String scanner) throws UnknownScanner, TException {

        try {
            scannerCache.invalidate(scanner);
        } catch (Exception e) {
            throw new TException(e.toString());
        }
    }

    @Override
    public void updateAndFlush(ByteBuffer login, String tableName, Map<ByteBuffer, List<ColumnUpdate>> cells) throws
            org.apache.accumulo.proxy.thrift.AccumuloException,
            org.apache.accumulo.proxy.thrift.AccumuloSecurityException,
            org.apache.accumulo.proxy.thrift.TableNotFoundException,
            org.apache.accumulo.proxy.thrift.MutationsRejectedException, TException {
        try {
            BatchWriter writer = getWriter(login, tableName, null);
            addCellsToWriter(cells, writer);
            writer.flush();
            writer.close();
        } catch (TableNotFoundException e) {
            throw new org.apache.accumulo.proxy.thrift.TableNotFoundException(e.toString());
        } catch (AccumuloSecurityException e) {
            throw new org.apache.accumulo.proxy.thrift.AccumuloSecurityException(e.toString());
        } catch (MutationsRejectedException e) {
            throw new org.apache.accumulo.proxy.thrift.MutationsRejectedException(e.toString());
        } catch (AccumuloException e) {
            throw new org.apache.accumulo.proxy.thrift.AccumuloException(e.toString());
        } catch (Exception e) {
            throw new TException(e.toString());
        }
    }

    private static final ColumnVisibility EMPTY_VIS = new ColumnVisibility();

    private void addCellsToWriter(Map<ByteBuffer,List<ColumnUpdate>> cells, BatchWriter writer) throws MutationsRejectedException {
        HashMap<Text,ColumnVisibility> vizMap = new HashMap<Text,ColumnVisibility>();

        for (Map.Entry<ByteBuffer,List<ColumnUpdate>> entry : cells.entrySet()) {
            Mutation m = new Mutation(new Text(ByteBufferUtil.toBytes(entry.getKey())));

            for (ColumnUpdate update : entry.getValue()) {
                ColumnVisibility viz = EMPTY_VIS;
                if (update.isSetColVisibility()) {
                    Text vizText = new Text(update.getColVisibility());
                    viz = vizMap.get(vizText);
                    if (viz == null) {
                        vizMap.put(vizText, viz = new ColumnVisibility(vizText));
                    }
                }
                byte[] value = new byte[0];
                if (update.isSetValue())
                    value = update.getValue();
                if (update.isSetTimestamp()) {
                    if (update.isSetDeleteCell()) {
                        m.putDelete(new Text(update.getColFamily()), new Text(update.getColQualifier()), viz, update.getTimestamp());
                    } else {
                        if (update.isSetDeleteCell()) {
                            m.putDelete(new Text(update.getColFamily()), new Text(update.getColQualifier()), viz, update.getTimestamp());
                        } else {
                            m.put(new Text(update.getColFamily()), new Text(update.getColQualifier()), viz, update.getTimestamp(), new Value(value));
                        }
                    }
                } else {
                    m.put(new Text(update.getColFamily()), new Text(update.getColQualifier()), viz, new Value(value));
                }
            }
            writer.addMutation(m);
        }
    }


    @Override
    public String createWriter(ByteBuffer login, String tableName, WriterOptions opts) throws
            org.apache.accumulo.proxy.thrift.AccumuloException,
            org.apache.accumulo.proxy.thrift.AccumuloSecurityException,
            org.apache.accumulo.proxy.thrift.TableNotFoundException, TException {
        try {
            BatchWriter writer = getWriter(login, tableName, opts);
            UUID uuid = UUID.randomUUID();
            writerCache.put(uuid, writer);
            return uuid.toString();

        } catch (TableNotFoundException e) {
            throw new org.apache.accumulo.proxy.thrift.TableNotFoundException(e.toString());
        } catch (AccumuloSecurityException e) {
            throw new org.apache.accumulo.proxy.thrift.AccumuloSecurityException(e.toString());
        } catch (AccumuloException e) {
            throw new org.apache.accumulo.proxy.thrift.AccumuloException(e.toString());
        } catch (Exception e) {
            throw new TException(e.toString());
        }
    }

    @Override
    public void update(String writer, Map<ByteBuffer, List<ColumnUpdate>> cells) {
        try {
            BatchWriter batchwriter = writerCache.getIfPresent(UUID.fromString(writer));
            addCellsToWriter(cells, batchwriter);
        } catch (MutationsRejectedException e) {    // this will be thrown again on flush() and close()
        }
    }

    @Override
    public void flush(String writer) throws UnknownWriter,
            org.apache.accumulo.proxy.thrift.MutationsRejectedException, TException {
        try {
            BatchWriter batchwriter = writerCache.getIfPresent(UUID.fromString(writer));
            if (batchwriter == null) {
                throw new UnknownWriter("Writer never existed or no longer exists");
            }
            batchwriter.flush();
        } catch (MutationsRejectedException e) {
            throw new org.apache.accumulo.proxy.thrift.MutationsRejectedException(e.toString());
        } catch (Exception e) {
            throw new TException(e.toString());
        }
    }

    @Override
    public void closeWriter(String writer) throws UnknownWriter,
            org.apache.accumulo.proxy.thrift.MutationsRejectedException, TException {
        try {
            BatchWriter batchwriter = writerCache.getIfPresent(UUID.fromString(writer));
            if (batchwriter == null) {
                throw new UnknownWriter("Writer never existed or no longer exists");
            }
            batchwriter.close();
            writerCache.invalidate(UUID.fromString(writer));
        } catch (MutationsRejectedException e) {
            throw new org.apache.accumulo.proxy.thrift.MutationsRejectedException(e.toString());
        } catch (Exception e) {
            throw new TException(e.toString());
        }
    }

    static private final ByteBuffer EMPTY = ByteBuffer.wrap(new byte[] {});

    @Override
    public Range getRowRange(ByteBuffer row) throws TException {
        return new org.apache.accumulo.proxy.thrift.Range(new org.apache.accumulo.proxy.thrift.Key(row, EMPTY, EMPTY, EMPTY), true,
                new org.apache.accumulo.proxy.thrift.Key(row, EMPTY, EMPTY, EMPTY), true);
    }

    private org.apache.accumulo.proxy.thrift.Range getRange(org.apache.accumulo.core.data.Range r) {
        return new org.apache.accumulo.proxy.thrift.Range(getProxyKey(r.getStartKey()), r.isStartKeyInclusive(), getProxyKey(r.getEndKey()), r.isEndKeyInclusive());
    }

    private org.apache.accumulo.proxy.thrift.Key getProxyKey(org.apache.accumulo.core.data.Key k) {
        if (k == null)
            return null;
        org.apache.accumulo.proxy.thrift.Key result = new org.apache.accumulo.proxy.thrift.Key(TextUtil.getByteBuffer(k.getRow()), TextUtil.getByteBuffer(k
                .getColumnFamily()), TextUtil.getByteBuffer(k.getColumnQualifier()), TextUtil.getByteBuffer(k.getColumnVisibility()));
        return result;
    }

    private org.apache.accumulo.core.data.Range getRange(org.apache.accumulo.proxy.thrift.Range range) {
        return new org.apache.accumulo.core.data.Range(Util.fromThrift(range.start), Util.fromThrift(range.stop));
    }

    @Override
    public Key getFollowing(Key key, PartialKey part) throws TException {
        org.apache.accumulo.core.data.Key key_ = Util.fromThrift(key);
        org.apache.accumulo.core.data.PartialKey part_ = org.apache.accumulo.core.data.PartialKey.valueOf(part.toString());
        org.apache.accumulo.core.data.Key followingKey = key_.followingKey(part_);
        return getProxyKey(followingKey);
    }

    private static final Long DEFAULT_MAX_MEMORY = 50 * 1024 * 1024l;
    private static final Long DEFAULT_MAX_LATENCY = 2 * 60 * 1000l;
    private static final Long DEFAULT_TIMEOUT = Long.MAX_VALUE;
    private static final Integer DEFAULT_MAX_WRITE_THREADS = 3;

    private BatchWriter getWriter(ByteBuffer login, String tableName, WriterOptions opts) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {

        if(opts == null) {
            opts = new WriterOptions();
        }

        if(opts.getMaxMemory() == 0) {
            opts.setMaxMemory(DEFAULT_MAX_MEMORY);
        }

        if(opts.getLatencyMs() == 0) {
            opts.setLatencyMs(DEFAULT_MAX_LATENCY);
        }

        if(opts.getTimeoutMs() == 0) {
            opts.setTimeoutMs(DEFAULT_TIMEOUT);
        }

        if(opts.getThreads() == 0) {
            opts.setThreads(DEFAULT_MAX_WRITE_THREADS);
        }
        return getConnector(login).createBatchWriter(tableName, opts.getMaxMemory(), opts.getLatencyMs(), opts.getThreads());
    }

    private org.apache.accumulo.core.client.IteratorSetting getIteratorSetting(org.apache.accumulo.proxy.thrift.IteratorSetting setting) {
        return new org.apache.accumulo.core.client.IteratorSetting(setting.priority, setting.name, setting.iteratorClass, setting.getProperties());
    }

    private IteratorUtil.IteratorScope getIteratorScope(org.apache.accumulo.proxy.thrift.IteratorScope scope) {
        return IteratorUtil.IteratorScope.valueOf(scope.toString().toLowerCase());
    }

    private EnumSet<IteratorUtil.IteratorScope> getIteratorScopes(Set<org.apache.accumulo.proxy.thrift.IteratorScope> scopes) {
        EnumSet<IteratorUtil.IteratorScope> scopes_ = EnumSet.noneOf(IteratorUtil.IteratorScope.class);
        for (org.apache.accumulo.proxy.thrift.IteratorScope scope : scopes) {
            scopes_.add(getIteratorScope(scope));
        }
        return scopes_;
    }

    private EnumSet<org.apache.accumulo.proxy.thrift.IteratorScope> getProxyIteratorScopes(Set<IteratorUtil.IteratorScope> scopes) {
        EnumSet<org.apache.accumulo.proxy.thrift.IteratorScope> scopes_ = EnumSet.noneOf(org.apache.accumulo.proxy.thrift.IteratorScope.class);
        for (IteratorUtil.IteratorScope scope : scopes) {
            scopes_.add(org.apache.accumulo.proxy.thrift.IteratorScope.valueOf(scope.toString().toUpperCase()));
        }
        return scopes_;
    }

    private Authorizations getAuthorizations(Set<ByteBuffer> authorizations) {
        List<String> auths = new ArrayList<String>();
        for (ByteBuffer bbauth : authorizations) {
            auths.add(ByteBufferUtil.toString(bbauth));
        }
        return new Authorizations(auths.toArray(new String[0]));
    }

}
