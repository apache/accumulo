package org.apache.accumulo.minicluster;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import com.google.common.base.Preconditions;

/**
 * Specified the desired numbers of servers by server type and resource group type.
 *
 * @since @3.1.0
 */
public class ResourceGroups {

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
        // TODO use constant
        Preconditions.checkArgument(resourceGroup.equals("default"),
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

  private ResourceGroups(Map<ResourceGroup,Integer> resourceGroupSizes) {
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

    /**
     * Sets the number of servers in the default resource group for a server type.
     */
    Builder put(ServerType type, int numServers);

    Builder put(ServerType type, String resourceGroup, int numServers);

    Builder put(Map<ResourceGroup,Integer> existingGoals);

    ResourceGroups build();
  }

  public static Builder builder() {
    var rgMap = new HashMap<ResourceGroup,Integer>();

    return new Builder() {

      @Override
      public Builder putDefaults() {
        put(ServerType.MANAGER, "default", 1);
        put(ServerType.GARBAGE_COLLECTOR, "default", 1);
        put(ServerType.ZOOKEEPER, "default", 1);
        put(ServerType.TABLET_SERVER, "default", 1);
        // TODO in elasticity will need to add compactors
        return this;
      }

      @Override
      public Builder put(ServerType type, int numServers) {
        // TODO use a constatnt
        return put(type, "default", numServers);
      }

      @Override
      public Builder put(ServerType type, String resourceGroup, int numServers) {
        Preconditions.checkArgument(numServers > 0, "Number of servers must be positive not : %s",
            numServers);
        rgMap.put(new ResourceGroup(type, resourceGroup), numServers);
        return this;
      }

      @Override
      public Builder put(Map<ResourceGroup,Integer> existingGoals) {
        // not calling rgMap.putAll so that input can be verified
        existingGoals.forEach((rg, ns) -> put(rg.getServerType(), rg.getResourceGroup(), ns));
        return this;
      }

      @Override
      public ResourceGroups build() {
        // TODO need to prevent further calls to put
        return new ResourceGroups(Map.copyOf(rgMap));
      }
    };
  }
}
