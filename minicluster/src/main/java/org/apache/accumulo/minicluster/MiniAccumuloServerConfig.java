package org.apache.accumulo.minicluster;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

import org.apache.accumulo.core.spi.scan.ScanServerSelector;

import com.google.common.base.Preconditions;

/**
 * An immutable, validated object that specifies the desired numbers of servers by server type and
 * resource group type.
 *
 * @since @3.1.0
 */
public class MiniAccumuloServerConfig {

  private static final String DEFAULT_RG = ScanServerSelector.DEFAULT_SCAN_SERVER_GROUP_NAME;

  public static class ResourceGroup {
    private final ServerType serverType;

    private final String resourceGroup;

    public ResourceGroup(ServerType serverType, String resourceGroup) {
      ;
      this.serverType = Objects.requireNonNull(serverType);
      this.resourceGroup = Objects.requireNonNull(resourceGroup);
      // TODO is there other validation that should be done for a resource group name?
      Preconditions.checkArgument(!resourceGroup.isBlank(),
          "Blank resource group name is not allowed");

      if (serverType == ServerType.ZOOKEEPER || serverType == ServerType.COMPACTION_COORDINATOR
          || serverType == ServerType.MANAGER || serverType == ServerType.GARBAGE_COLLECTOR
          || serverType == ServerType.MONITOR) {
        Preconditions.checkArgument(resourceGroup.equals(DEFAULT_RG),
            "For server type %s can only use default resource group not : %s", serverType,
            resourceGroup);
      }
    }

    public ServerType getServerType() {
      return serverType;
    }

    public String getResourceGroup() {
      return resourceGroup;
    }

    @Override
    public int hashCode() {
      return Objects.hash(serverType, resourceGroup);
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof ResourceGroup) {
        var otherRG = (ResourceGroup) o;
        return serverType.equals(otherRG.serverType) && resourceGroup.equals(otherRG.resourceGroup);
      }

      return false;
    }
  }

  private final Map<ResourceGroup,Integer> resourceGroupSizes;

  private MiniAccumuloServerConfig(Map<ResourceGroup,Integer> resourceGroupSizes) {
    this.resourceGroupSizes = resourceGroupSizes;
  }

  public Map<ResourceGroup,Integer> getGroupSizes() {
    return resourceGroupSizes;
  }

  public interface Builder {
    /**
     * Populates default resource group configuration. TODO document what the defaults are.
     */
    Builder putDefaults();

    Builder putDefaultResourceGroup(ServerType serverType, int numServers);

    Builder putCompactorResourceGroup(String resourceGroupName, int numCompactors);

    Builder putScanServerResourceGroup(String resourceGroupName, int numScanServers);

    Builder putTabletServerResourceGroup(String resourceGroupName, int numTabletServers);

    Builder put(MiniAccumuloServerConfig existingGroups);

    /**
     * Removes any groups previously added that match the predicate. Useful in conjunction with
     * {@link #put(MiniAccumuloServerConfig)}
     */
    Builder removeIf(Predicate<ResourceGroup> groupPredicate);

    MiniAccumuloServerConfig build();
  }

  public static Builder builder() {
    var rgMap = new HashMap<ResourceGroup,Integer>();

    return new Builder() {

      @Override
      public Builder putDefaults() {
        put(ServerType.MANAGER, DEFAULT_RG, 1);
        put(ServerType.GARBAGE_COLLECTOR, DEFAULT_RG, 1);
        put(ServerType.ZOOKEEPER, DEFAULT_RG, 1);
        put(ServerType.TABLET_SERVER, DEFAULT_RG, 1);
        // TODO in elasticity will need to add compactors
        return this;
      }

      @Override
      public Builder putDefaultResourceGroup(ServerType serverType, int numServers) {
        return put(serverType, DEFAULT_RG, numServers);
      }

      @Override
      public Builder putCompactorResourceGroup(String resourceGroupName, int numCompactors) {
        return put(ServerType.COMPACTOR, resourceGroupName, numCompactors);
      }

      @Override
      public Builder putScanServerResourceGroup(String resourceGroupName, int numScanServers) {
        return put(ServerType.SCAN_SERVER, resourceGroupName, numScanServers);
      }

      @Override
      public Builder putTabletServerResourceGroup(String resourceGroupName, int numTabletServers) {
        return put(ServerType.TABLET_SERVER, resourceGroupName, numTabletServers);
      }

      private Builder put(ServerType type, String resourceGroup, int numServers) {
        Preconditions.checkArgument(numServers > 0, "Number of servers must be positive not : %s",
            numServers);
        rgMap.put(new ResourceGroup(type, resourceGroup), numServers);
        return this;
      }

      @Override
      public Builder put(MiniAccumuloServerConfig existingGroups) {
        // not calling rgMap.putAll so that input can be verified
        existingGroups.getGroupSizes()
            .forEach((rg, ns) -> put(rg.getServerType(), rg.getResourceGroup(), ns));
        return this;
      }

      @Override
      public Builder removeIf(Predicate<ResourceGroup> groupPredicate) {
        rgMap.keySet().removeIf(groupPredicate);
        return this;
      }

      @Override
      public MiniAccumuloServerConfig build() {
        // TODO need to prevent further calls to put
        return new MiniAccumuloServerConfig(Map.copyOf(rgMap));
      }
    };
  }
}
