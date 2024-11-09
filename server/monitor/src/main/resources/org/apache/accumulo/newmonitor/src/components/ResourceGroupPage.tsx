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
import { useState, useEffect } from 'react';
import { useParams, Link } from 'react-router-dom';
import {
  fetchAllMetrics,
  fetchCompactorsSummary,
  fetchScanServerSummary,
  fetchTabletServerSummary,
} from '../api';
import { ServerData, ServerTypeDisplayName, SummaryMetrics } from '../types';
import { Table } from 'react-bootstrap';

function ResourceGroupPage() {
  const { rgName } = useParams<{ rgName: string }>(); // grab the resource group name from the URL
  const [servers, setServers] = useState<ServerData[]>([]);
  const [compactorMetrics, setCompactorMetrics] = useState<SummaryMetrics>({});
  const [sserverMetrics, setSserverMetrics] = useState<SummaryMetrics>({});
  const [tserverMetrics, setTserverMetrics] = useState<SummaryMetrics>({});

  useEffect(() => {
    async function getData() {
      const allMetrics: ServerData[] = await fetchAllMetrics();
      const filteredServers: ServerData[] = allMetrics.filter((s) => s.resourceGroup === rgName);
      setServers(filteredServers);

      const [compactorData, sserverData, tserverData] = await Promise.all([
        fetchCompactorsSummary(rgName),
        fetchScanServerSummary(rgName),
        fetchTabletServerSummary(rgName),
      ]);

      setCompactorMetrics(compactorData);
      setSserverMetrics(sserverData);
      setTserverMetrics(tserverData);
    }
    void getData();
  }, [rgName]);

  return (
    <div>
      <h1>Resource Group: {rgName}</h1>
      <h2>Components in this Resource Group:</h2>
      {servers.length === 0 ? (
        <p>No components found in this resource group.</p>
      ) : (
        <Table striped bordered hover size="lg" className="mx-auto">
          <thead>
            <tr>
              <th>Host</th>
              <th>Server Type</th>
            </tr>
          </thead>
          <tbody>
            {servers.map((server) => (
              <tr key={server.host}>
                <td>
                  <Link to={`/server/${server.host}`}>{server.host}</Link>
                </td>
                <td>{ServerTypeDisplayName.get(server.serverType)}</td>
              </tr>
            ))}
          </tbody>
        </Table>
      )}

      <h2>Compactor Metrics</h2>
      {Object.keys(compactorMetrics).length === 0 ? (
        <p>No compactor metrics available for this resource group.</p>
      ) : (
        <Table striped bordered hover size="lg" className="mx-auto">
          <thead>
            <tr>
              <th>Metric Name</th>
              <th>Count</th>
              <th>Mean</th>
              <th>Max</th>
            </tr>
          </thead>
          <tbody>
            {Object.entries(compactorMetrics).map(([metricName, stats]) => (
              <tr key={metricName}>
                <td>{metricName}</td>
                <td>{stats.count}</td>
                <td>{stats.mean}</td>
                <td>{stats.max}</td>
              </tr>
            ))}
          </tbody>
        </Table>
      )}

      <h2>Scan Server Metrics</h2>
      {Object.keys(sserverMetrics).length === 0 ? (
        <p>No scan server metrics available for this resource group.</p>
      ) : (
        <Table striped bordered hover size="lg" className="mx-auto">
          <thead>
            <tr>
              <th>Metric Name</th>
              <th>Count</th>
              <th>Mean</th>
              <th>Max</th>
            </tr>
          </thead>
          <tbody>
            {Object.entries(sserverMetrics).map(([metricName, stats]) => (
              <tr key={metricName}>
                <td>{metricName}</td>
                <td>{stats.count}</td>
                <td>{stats.mean}</td>
                <td>{stats.max}</td>
              </tr>
            ))}
          </tbody>
        </Table>
      )}

      <h2>Tablet Server Metrics</h2>
      {Object.keys(tserverMetrics).length === 0 ? (
        <p>No tablet server metrics available for this resource group.</p>
      ) : (
        <Table striped bordered hover size="lg" className="mx-auto">
          <thead>
            <tr>
              <th>Metric Name</th>
              <th>Count</th>
              <th>Mean</th>
              <th>Max</th>
            </tr>
          </thead>
          <tbody>
            {Object.entries(tserverMetrics).map(([metricName, stats]) => (
              <tr key={metricName}>
                <td>{metricName}</td>
                <td>{stats.count}</td>
                <td>{stats.mean}</td>
                <td>{stats.max}</td>
              </tr>
            ))}
          </tbody>
        </Table>
      )}
    </div>
  );
}

export default ResourceGroupPage;
