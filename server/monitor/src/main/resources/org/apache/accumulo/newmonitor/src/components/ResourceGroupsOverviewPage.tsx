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
import { Card, Row, Col, Container } from 'react-bootstrap';
import { Link } from 'react-router-dom';
import { fetchDeploymentMetrics } from '../api';
import { DeploymentsMetrics, ServerType, ServerTypeDisplayName } from '../types';

function ResourceGroupsOverviewPage() {
  const [deploymentMetrics, setDeploymentMetrics] = useState<DeploymentsMetrics>({});

  useEffect(() => {
    async function getDeploymentMetrics() {
      try {
        const data = await fetchDeploymentMetrics();
        setDeploymentMetrics(data);
      } catch (error) {
        console.error('Error fetching deployment metrics:', error);
      }
    }
    void getDeploymentMetrics();
  }, []);

  return (
    <Container>
      <h1 className="text-center my-4">Resource Groups Deployment Metrics</h1>
      <Row xs={1} md={2} className="g-4">
        {Object.entries(deploymentMetrics).map(([resourceGroup, serverTypes]) => {
          const hasUnresponsiveHosts = Object.values(serverTypes).some(
            (metrics) => metrics.notRespondedHosts.length > 0
          );

          return (
            <Col key={resourceGroup}>
              <Card className={`h-100 border-3 ${hasUnresponsiveHosts ? 'border-danger-subtle' : 'border-success-subtle'}`}>
                <Card.Body>
                  <Card.Title>
                    <Link to={`/resource-groups/${resourceGroup}`}>{resourceGroup}</Link>
                  </Card.Title>
                  <div style={{ maxHeight: '300px', overflowX: 'hidden' }}>
                    <Row xs={1} sm={2} className="g-3">
                      {Object.entries(serverTypes).map(([serverType, metrics]) => {
                        const hasUnresponsiveHosts = metrics.notRespondedHosts.length > 0;

                        return (
                          <Col key={`${resourceGroup}-${serverType}`}>
                            <Card className={`border-3 ${hasUnresponsiveHosts ? 'bg-danger-subtle' : 'border-success-subtle'}`}>
                              <Card.Body>
                                <Card.Title className="mb-2">
                                  {ServerTypeDisplayName.get(serverType as ServerType)}
                                </Card.Title>
                                <ul className="list-unstyled mb-0">
                                  <li>
                                    Configured: {metrics.configured}
                                  </li>
                                  <li>
                                    Responded: {metrics.responded}
                                  </li>
                                  {hasUnresponsiveHosts ? (
                                    <>
                                      <strong>Unresponsive Hosts:</strong>
                                      <ul className="list-unstyled">
                                        {metrics.notRespondedHosts.map((host) => (
                                          <li key={host}>
                                            <Link to={`/server/${host}`}>{host}</Link>
                                          </li>
                                        ))}
                                      </ul>
                                    </>
                                  ) : (
                                    <li>Unresponsive: 0</li>
                                  )}
                                </ul>
                              </Card.Body>
                            </Card>
                          </Col>
                        );
                      })}
                    </Row>
                  </div>
                </Card.Body>
              </Card>
            </Col>
          );
        })}
      </Row>
    </Container>
  );
}

export default ResourceGroupsOverviewPage;