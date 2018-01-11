/*
 * Copyright (c) 2018 OBiBa. All rights reserved.
 *
 * This program and the accompanying materials
 * are made available under the terms of the GNU Public License v3.0.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.obiba.es.mica;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.obiba.es.mica.mapping.*;
import org.obiba.mica.spi.search.ConfigurationProvider;
import org.obiba.mica.spi.search.Indexer;
import org.obiba.mica.spi.search.SearchEngineService;
import org.obiba.mica.spi.search.Searcher;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public class ESSearchEngineService implements SearchEngineService {

  private static final String ES_BRANCH = "2.4.x";

  private static final String ES_CONFIG_FILE = "elasticsearch.yml";

  private Properties properties;

  private boolean running;

  private Node esNode;

  private Client client;

  private ESIndexer esIndexer;

  private ESSearcher esSearcher;

  private ConfigurationProvider configurationProvider;

  private Set<Indexer.IndexConfigurationListener> indexConfigurationListeners;

  @Override
  public String getName() {
    return "mica-search-es";
  }

  @Override
  public Properties getProperties() {
    return properties;
  }

  @Override
  public void configure(Properties properties) {
    this.properties = properties;
  }

  @Override
  public boolean isRunning() {
    return running;
  }

  @Override
  public void start() {
    // do init stuff
    if (properties != null) {
      Settings.Builder builder = getSettings();
      if (isTransportClient())
        createTransportClient(builder);
      else
        createNodeClient(builder);

      esIndexer = new ESIndexer(this);
      esSearcher = new ESSearcher(this);


      running = true;
    }
  }

  @Override
  public void stop() {
    running = false;
    if (esNode != null) esNode.close();
    if (client != null) client.close();
    esNode = null;
    client = null;
  }

  @Override
  public void setConfigurationProvider(ConfigurationProvider configurationProvider) {
    this.configurationProvider = configurationProvider;
  }

  @Override
  public Indexer getIndexer() {
    return esIndexer;
  }

  @Override
  public Searcher getSearcher() {
    return esSearcher;
  }

  public Client getClient() {
    return client;
  }

  ConfigurationProvider getConfigurationProvider() {
    return configurationProvider;
  }

  ObjectMapper getObjectMapper() {
    if (configurationProvider == null || configurationProvider.getObjectMapper() == null) {
      ObjectMapper mapper = new ObjectMapper();
      mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
      mapper.findAndRegisterModules();
      return mapper;
    }
    return configurationProvider.getObjectMapper();
  }

  synchronized Set<Indexer.IndexConfigurationListener> getIndexConfigurationListeners() {
    if (indexConfigurationListeners == null) {
      indexConfigurationListeners = Sets.newHashSet();
      indexConfigurationListeners.add(new VariableIndexConfiguration(configurationProvider));
      indexConfigurationListeners.add(new DatasetIndexConfiguration(configurationProvider));
      indexConfigurationListeners.add(new StudyIndexConfiguration(configurationProvider));
      indexConfigurationListeners.add(new NetworkIndexConfiguration(configurationProvider));
      indexConfigurationListeners.add(new FileIndexConfiguration(configurationProvider));
      indexConfigurationListeners.add(new PersonIndexConfiguration(configurationProvider));
      indexConfigurationListeners.add(new ProjectIndexConfiguration(configurationProvider));
      indexConfigurationListeners.add(new TaxonomyIndexConfiguration(configurationProvider));
    }
    return indexConfigurationListeners;
  }

  int getNbShards() {
    return Integer.parseInt(properties.getProperty("shards", "5"));
  }

  int getNbReplicas() {
    return Integer.parseInt(properties.getProperty("replicas", "1"));
  }

  //
  // Private methods
  //

  private void createNodeClient(Settings.Builder builder) {
    esNode = NodeBuilder.nodeBuilder() //
        .client(!isDataNode()) //
        .settings(builder) //
        .clusterName(getClusterName()) //
        .node();
    client = esNode.client();
  }

  private void createTransportClient(Settings.Builder builder) {
    builder.put("client.transport.sniff", isTransportSniff());
    final TransportClient transportClient = TransportClient.builder().settings(builder.build()).build();
    getTransportAddresses().forEach(ta -> {
      int port = 9300;
      String host = ta;
      int sepIdx = ta.lastIndexOf(':');

      if (sepIdx > 0) {
        port = Integer.parseInt(ta.substring(sepIdx + 1, ta.length()));
        host = ta.substring(0, sepIdx);
      }

      try {
        transportClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));
      } catch (UnknownHostException e) {
        Throwables.propagate(e);
      }
    });
    client = transportClient;
  }

  private boolean isDataNode() {
    return Boolean.parseBoolean(properties.getProperty("dataNode", "true"));
  }

  private String getClusterName() {
    return properties.getProperty("clusterName", "mica");
  }

  private List<String> getTransportAddresses() {
    String addStr = properties.getProperty("transportAddresses", "").trim();
    return addStr.isEmpty() ? Lists.newArrayList() :
        Stream.of(addStr.split(",")).map(String::trim).collect(toList());
  }

  private boolean isTransportClient() {
    return Boolean.parseBoolean(properties.getProperty("transportClient", "false"));
  }

  private boolean isTransportSniff() {
    return Boolean.parseBoolean(properties.getProperty("transportSniff", "false"));
  }


  private File getWorkFolder() {
    return getServiceFolder(WORK_DIR_PROPERTY);
  }

  private File getInstallFolder() {
    return getServiceFolder(INSTALL_DIR_PROPERTY);
  }

  private File getServiceFolder(String dirProperty) {
    String defaultDir = new File(".").getAbsolutePath();
    String dataDirPath = properties.getProperty(dirProperty, defaultDir);
    File dataDir = new File(dataDirPath);
    if (!dataDir.exists()) dataDir.mkdirs();
    return dataDir;
  }

  Settings getIndexSettings() {
    return getSettings().build().getByPrefix("index.");
  }

  private Settings.Builder getSettings() {
    File pluginWorkDir = new File(getWorkFolder(), properties.getProperty("es.version", ES_BRANCH));
    Settings.Builder builder = Settings.settingsBuilder() //
        .put("path.home", getInstallFolder().getAbsolutePath()) //
        .put("path.data", new File(pluginWorkDir, "data").getAbsolutePath()) //
        .put("path.work", new File(pluginWorkDir, "work").getAbsolutePath());

    File defaultSettings = new File(getInstallFolder(), ES_CONFIG_FILE);
    if (defaultSettings.exists())
      builder.loadFromPath(defaultSettings.toPath());

    builder.loadFromSource(properties.getProperty("settings", ""))
        .put("cluster.name", getClusterName());
    return builder;
  }
}
