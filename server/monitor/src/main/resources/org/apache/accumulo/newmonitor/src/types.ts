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

export enum ServerType {
  GARBAGE_COLLECTOR = 'GARBAGE_COLLECTOR',
  TABLET_SERVER = 'TABLET_SERVER',
  SCAN_SERVER = 'SCAN_SERVER',
  MANAGER = 'MANAGER',
  COMPACTOR = 'COMPACTOR',
}

// Map of ServerType to how we want to display the string in the UI
export const ServerTypeDisplayName = new Map<ServerType, string>([
  [ServerType.GARBAGE_COLLECTOR, 'Garbage Collector'],
  [ServerType.TABLET_SERVER, 'Tablet Server'],
  [ServerType.SCAN_SERVER, 'Scan Server'],
  [ServerType.MANAGER, 'Manager'],
  [ServerType.COMPACTOR, 'Compactor'],
]);

export interface Tag {
  key: string;
  value: string;
}

export interface Metric {
  name: string;
  type: string;
  tags: Tag[];
  value: number;
}

export interface ServerData {
  timestamp: number;
  serverType: ServerType;
  resourceGroup: string;
  host: string;
  metrics: Metric[];
}

export type SummaryMetrics = Record<string, {
  count: number;
  mean: number;
  max: number;
}>;

export interface InstanceMetrics {
  instanceName: string;
  instanceUUID: string;
  zooKeepers: string[];
  volumes: string[];
}

export interface CompactionsMetrics {
  groupName: string;
  compactor: string;
  updates: Record<number, {
    status: string;
    details: string;
  }>;
  job: {
    jobId: string;
    status: string;
  };
}

export interface ProblemMetrics {
  host: string;
  port: number;
}

export interface TableMetrics {
  totalEntries: number;
  totalSizeOnDisk: number;
  totalFiles: number;
  totalWals: number;
  totalTablets: number;
  availableAlways: number;
  availableOnDemand: number;
  availableNever: number;
  totalAssignedTablets: number;
  totalAssignedToDeadServerTablets: number;
  totalHostedTablets: number;
  totalSuspendedTablets: number;
  totalUnassignedTablets: number;
}

export interface TabletMetrics {
  tabletId: string;
  numFiles: number;
  numWalLogs: number;
  estimatedEntries: number;
  tabletState: string;
  tabletDir: string;
  tabletAvailability: string;
  estimatedSize: number;
  location: string;
}

export type TablesMetrics = Record<string, TableMetrics>;

interface ServerMetrics {
  configured: number;
  responded: number;
  notResponded: number;
  notRespondedHosts: string[];
}

export type DeploymentsMetrics = Record<string, Record<ServerType, ServerMetrics>>;