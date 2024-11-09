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
import { useEffect, useState } from 'react';
import {
  fetchHealthStatus,
  fetchWarnings,
  fetchResourceGroupDetails,
  fetchInstanceMetrics,
} from '../api';
import { Link } from 'react-router-dom';
import {
  InstanceMetrics,
  HealthStatus,
  Warning,
  ResourceGroupDetails,
} from '../types';
import {
  Container,
  Row,
  Col,
  Alert,
  Card,
  ListGroup,
} from 'react-bootstrap';

function HomePage() {
  const [healthStatus, setHealthStatus] = useState<HealthStatus>('healthy');
  const [warnings, setWarnings] = useState<Warning[]>([]);
  const [resourceGroups, setResourceGroups] = useState<ResourceGroupDetails[]>([]);
  const [instanceMetrics, setInstanceMetrics] = useState<InstanceMetrics | null>(null);

  useEffect(() => {
    async function getData() {
      const health: HealthStatus = await fetchHealthStatus();
      setHealthStatus(health);

      const warningsData: Warning[] = await fetchWarnings();
      setWarnings(warningsData);

      const resourceGroupsData: ResourceGroupDetails[] = await fetchResourceGroupDetails();
      setResourceGroups(resourceGroupsData);

      const instanceData: InstanceMetrics = await fetchInstanceMetrics();
      setInstanceMetrics(instanceData);
    }
    void getData();
  }, []);

  return (
    <Container className="homepage-container text-center">
      <Row>
        <Col md={6}>
          <Card className="section-card">
            <Card.Body>
              <h1>Overall Health</h1>
              <Alert
                variant={
                  healthStatus === 'healthy'
                    ? 'success'
                    : healthStatus === 'degraded'
                      ? 'warning'
                      : 'danger'
                }
              >
                {healthStatus}
              </Alert>
              <ul>
                {warnings.map((warning, index) => (
                  <li key={index}>
                    {warning.message}{' '}
                    <Link to={`/resource-groups/${warning.resourceGroup}`}>
                      {warning.resourceGroup}
                    </Link>
                  </li>
                ))}
              </ul>
            </Card.Body>
          </Card>
        </Col>
        <Col md={6}>
          <Card className="section-card">
            <Card.Body>
              <h1>Resource Groups</h1>
              <div className="resource-groups-scroll">
                {resourceGroups.map((group) => (
                  <Card key={group.name} className="mb-2">
                    <Card.Body>
                      <Card.Title>
                        <Link to={`/resource-groups/${group.name}`}>{group.name}</Link>
                      </Card.Title>
                      <div>
                        <Alert
                          variant={
                            group.healthStatus === 'healthy'
                              ? 'success'
                              : group.healthStatus === 'degraded'
                                ? 'warning'
                                : 'danger'
                          }
                        >
                          {group.healthStatus}
                        </Alert>
                        <p>Type: {group.type}</p>
                        <p>Component Count: {group.componentCount}</p>
                      </div>
                    </Card.Body>
                  </Card>
                ))}
              </div>
            </Card.Body>
          </Card>
        </Col>
      </Row>
      <Row>
        <Col>
          <Card className="section-card">
            <Card.Body>
              <h1>Cluster Info</h1>
              {instanceMetrics && (
                <Row>
                  <Col md={6}>
                    <h2>Zookeeper Servers</h2>
                    <ListGroup>
                      {instanceMetrics.zooKeepers.map((zk, index) => (
                        <ListGroup.Item key={index}>{zk}</ListGroup.Item>
                      ))}
                    </ListGroup>
                  </Col>
                  <Col md={6}>
                    <h2>Volumes</h2>
                    <ListGroup>
                      {instanceMetrics.volumes.map((volume, index) => (
                        <ListGroup.Item key={index}>{volume}</ListGroup.Item>
                      ))}
                    </ListGroup>
                  </Col>
                </Row>
              )}
            </Card.Body>
          </Card>
        </Col>
      </Row>
    </Container>
  );
}

export default HomePage;