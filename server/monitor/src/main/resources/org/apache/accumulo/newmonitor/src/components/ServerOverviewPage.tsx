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
import { fetchAllMetrics } from '../api';
import { ServerData, ServerType, ServerTypeDisplayName } from '../types';
import { Table } from 'react-bootstrap';

function ServerOverviewPage() {
  const { serverType } = useParams<{ serverType?: string }>();
  const [servers, setServers] = useState<ServerData[]>([]);

  useEffect(() => {
    async function getServers() {
      const data = await fetchAllMetrics();
      const filteredServers = serverType
        ? data.filter((s) => s.serverType === serverType.toUpperCase() as ServerType)
        : data;
      setServers(filteredServers);
    }
    void getServers();
  }, [serverType]);

  return (
    <div className="text-center">
      <h1>{ServerTypeDisplayName.get(serverType as ServerType)} Overview</h1>
      <br></br>
      {servers.length === 0 ? (
        <p>No servers available.</p>
      ) : (
        <Table striped bordered hover size="lg" className="mx-auto">
          <thead>
            <tr>
              <th>Host</th>
              <th>Type</th>
              <th>Resource Group</th>
            </tr>
          </thead>
          <tbody>
            {servers.map((server) => (
              <tr key={server.host}>
                <td>
                  <Link to={`/server/${server.host}`}>{server.host}</Link>
                </td>
                <td>{ServerTypeDisplayName.get(server.serverType)}</td>
                <td>
                  <Link to={`/resource-groups/${server.resourceGroup}`}>
                    {server.resourceGroup}
                  </Link>
                </td>
              </tr>
            ))}
          </tbody>
        </Table>
      )}
    </div>
  );
}

export default ServerOverviewPage;
