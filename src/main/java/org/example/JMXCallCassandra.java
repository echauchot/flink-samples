package org.example;

import org.apache.cassandra.service.StorageServiceMBean;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import java.rmi.server.RMISocketFactory;
import java.util.HashMap;
import java.util.Map;

public class JMXCallCassandra {

  private static final String STORAGE_SERVICE_MBEAN =
    "org.apache.cassandra.db:type=StorageService";
  private static final int JMX_PORT = 7199;
  private static final long FLUSH_MEMTABLES_DELAY = 3000L;
  private static final String HOST = "localhost";
  private static final String JMX_USERNAME = "cassandra";
  private static final String JMX_PASSWORD = "cassandra";
  private static final String KEYSPACE = "flink";
  private static final String TABLE = "test";

  public static void main(String[] args) throws Exception {
    flushMemTables();
  }

  /**
   * Force the flush of cassandra memTables to SSTables in order to update size_estimates. This
   * flush method is what official Cassandra NoteTool does.
   */
  private static void flushMemTables() throws Exception {
    JMXServiceURL url =
      new JMXServiceURL(
        String.format(
          "service:jmx:rmi://%s/jndi/rmi://%s:%d/jmxrmi",
          HOST, HOST, JMX_PORT));
    Map<String, Object> env = new HashMap<>();
    String[] creds = {JMX_USERNAME, JMX_PASSWORD};
    env.put(JMXConnector.CREDENTIALS, creds);
    env.put(
      "com.sun.jndi.rmi.factory.socket",
      RMISocketFactory.getDefaultSocketFactory()); // connection without ssl
    JMXConnector jmxConnector = JMXConnectorFactory.connect(url, env);
    MBeanServerConnection mBeanServerConnection = jmxConnector.getMBeanServerConnection();
    ObjectName objectName = new ObjectName(STORAGE_SERVICE_MBEAN);
    StorageServiceMBean mBeanProxy =
      JMX.newMBeanProxy(mBeanServerConnection, objectName, StorageServiceMBean.class);
    mBeanProxy.forceKeyspaceFlush(KEYSPACE, TABLE);
    jmxConnector.close();
    Thread.sleep(FLUSH_MEMTABLES_DELAY);
  }
}
