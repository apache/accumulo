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
import { fetchAllMetrics, fetchManagerMetrics, fetchGCMetrics } from '../api';
import { ServerData, ServerTypeDisplayName } from '../types';
import { Table } from 'react-bootstrap';

function ServerPage() {
  const { serverId } = useParams<{ serverId: string }>();
  const [serverData, setServerData] = useState<ServerData | null>(null);

  useEffect(() => {
    async function getServerData() {
      let data: ServerData | null = null;
      if (serverId === 'MANAGER') {
        data = await fetchManagerMetrics();
      } else if (serverId === 'GARBAGE_COLLECTOR') {
        data = await fetchGCMetrics();
      } else {
        const allData = await fetchAllMetrics();
        data = allData.find((s) => s.host === serverId) ?? null;
      }

      // remove "accumulo." from the name of each metric
      if (data) {
        data.metrics.forEach((metric) => {
          metric.name = metric.name.replace('accumulo.', '');
        });
      }

      setServerData(data);
    }
    void getServerData();
  }, [serverId]);

  if (!serverData) {
    return <div>No data found...</div>;
  }

  return (
    <div>
      <h1>{ServerTypeDisplayName.get(serverData.serverType)}</h1>
      <h4>Hostname: {serverData.host}</h4>
      <h4>
        Resource Group:{' '}
        <Link to={`/resource-groups/${serverData.resourceGroup}`}>
          {serverData.resourceGroup}
        </Link>
      </h4>
      <br></br>
      <h2>Metrics:</h2>
      <Table striped bordered hover size="lg" className="mx-auto">
        <thead>
          <tr>
            <th>Name</th>
            <th>Value</th>
            <th>Type</th>
          </tr>
        </thead>
        <tbody>
          {serverData.metrics.map((metric, index) => (
            <tr key={index}>
              <td>{metric.name}</td>
              <td>{metric.value}</td>
              <td>{metric.type}</td>
            </tr>
          ))}
        </tbody>
      </Table>
    </div>
  );
}

export default ServerPage;
