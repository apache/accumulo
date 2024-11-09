/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import {
  ServerData,
  SummaryMetrics,
  InstanceMetrics,
  CompactionsMetrics,
  ProblemMetrics,
  HealthStatus,
  Warning,
  ResourceGroupDetails,
  TablesMetrics,
  DeploymentsMetrics,
} from './types';

async function fetchWithHandling<T>(
  path: string,
  options?: { returnEmptyOn404?: boolean }
): Promise<T> {
  const response = await fetch(path);

  if (response.status === 404) {
    if (options?.returnEmptyOn404) {
      // Resource not found but should return empty data
      return {} as T;
    }
    // Resource not found, can throw an error or return empty data
    throw new Error(`Resource not found at ${path}`);
  }

  if (response.status === 500) {
    // Internal Server Error
    throw new Error(`Server error at ${path}: ${response.statusText}`);
  }

  if (!response.ok) {
    // Other non-OK responses
    throw new Error(`Failed to fetch ${path}: ${response.statusText}`);
  }

  return await response.json() as T;
}

export async function fetchAllMetrics(): Promise<ServerData[]> {
  const path = '/metrics';
  return await fetchWithHandling<ServerData[]>(path);
}

export async function fetchInstanceMetrics(): Promise<InstanceMetrics> {
  const path = '/metrics/instance';
  return await fetchWithHandling<InstanceMetrics>(path);
}

export async function fetchGroups(): Promise<string[]> {
  const path = '/metrics/groups';
  return await fetchWithHandling<string[]>(path);
}

export async function fetchManagerMetrics(): Promise<ServerData> {
  const path = '/metrics/manager';
  return await fetchWithHandling<ServerData>(path);
}

export async function fetchGCMetrics(): Promise<ServerData> {
  const path = '/metrics/gc';
  return await fetchWithHandling<ServerData>(path);
}

export async function fetchCompactorsSummary(group?: string): Promise<SummaryMetrics> {
  const path = group ? `/metrics/compactors/summary/${group}` : '/metrics/compactors/summary';
  return await fetchWithHandling<SummaryMetrics>(path, { returnEmptyOn404: true });
}

export async function fetchCompactorsDetail(group: string): Promise<ServerData[]> {
  const path = `/metrics/compactors/detail/${group}`;
  return await fetchWithHandling<ServerData[]>(path, { returnEmptyOn404: true });
}

export async function fetchScanServerSummary(group?: string): Promise<SummaryMetrics> {
  const path = group ? `/metrics/sservers/summary/${group}` : '/metrics/sservers/summary';
  return await fetchWithHandling<SummaryMetrics>(path, { returnEmptyOn404: true });
}

export async function fetchScanServerDetail(group: string): Promise<ServerData[]> {
  const path = `/metrics/sservers/detail/${group}`;
  return await fetchWithHandling<ServerData[]>(path, { returnEmptyOn404: true });
}

export async function fetchTabletServerSummary(group?: string): Promise<SummaryMetrics> {
  const path = group ? `/metrics/tservers/summary/${group}` : '/metrics/tservers/summary';
  return await fetchWithHandling<SummaryMetrics>(path, { returnEmptyOn404: true });
}

export async function fetchTabletServerDetail(group: string): Promise<ServerData[]> {
  const path = `/metrics/tservers/detail/${group}`;
  return await fetchWithHandling<ServerData[]>(path, { returnEmptyOn404: true });
}

export async function fetchProblems(): Promise<ProblemMetrics> {
  const path = '/metrics/problems';
  return await fetchWithHandling<ProblemMetrics>(path);
}

export async function fetchCompactions(max?: number): Promise<CompactionsMetrics> {
  const path = max ? `/metrics/compactions/${max}` : '/metrics/compactions';
  return await fetchWithHandling<CompactionsMetrics>(path);
}

export async function fetchTablesMetrics(name?: string): Promise<TablesMetrics> {
  const path = name ? `/metrics/tables/${name}` : '/metrics/tables';
  return await fetchWithHandling<TablesMetrics>(path);
}

export async function fetchDeploymentMetrics(): Promise<DeploymentsMetrics> {
  const path = '/metrics/deployment';
  return await fetchWithHandling<DeploymentsMetrics>(path);
}

// TEST DATA FOR HOMEPAGE:

export async function fetchHealthStatus(): Promise<HealthStatus> {
  // Simulate fetching health status with a random value
  const statuses: HealthStatus[] = ['healthy', 'degraded', 'unhealthy'];
  const randomStatus = statuses[Math.floor(Math.random() * statuses.length)];
  return Promise.resolve(randomStatus);
}

export async function fetchWarnings(): Promise<Warning[]> {
  // Return dummy warnings
  return Promise.resolve([
    {
      resourceGroup: 'default',
      message: 'There are warnings in RG default',
    },
    {
      resourceGroup: 'scansgroup',
      message: 'There are warnings in RG scansgroup',
    },
  ]);
}

export async function fetchResourceGroupDetails(): Promise<ResourceGroupDetails[]> {
  // Return dummy resource group details
  return Promise.resolve([
    {
      name: 'default',
      healthStatus: 'degraded',
      type: 'N/A',
      componentCount: 9,
    },
    {
      name: 'scansgroup',
      healthStatus: 'healthy',
      type: 'scan server',
      componentCount: 5,
    },
    {
      name: 'group 4',
      healthStatus: 'unhealthy',
      type: 'tablet server',
      componentCount: 2,
    },
  ]);
}