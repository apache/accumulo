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
  fetchInstanceMetrics,
  fetchTablesMetrics,
} from '../api';
import {
  InstanceMetrics,
  TablesMetrics,
} from '../types';
import {
  Container,
  Row,
  Col,
  Card,
  ListGroup,
} from 'react-bootstrap';
import TablesOverviewTable from './TablesOverviewTable';

function HomePage() {
  const [instanceMetrics, setInstanceMetrics] = useState<InstanceMetrics | null>(null);
  const [tableData, setTableData] = useState<TablesMetrics>({});

  useEffect(() => {
    async function getData() {
      const instanceData: InstanceMetrics = await fetchInstanceMetrics();
      setInstanceMetrics(instanceData);

      const data: TablesMetrics = await fetchTablesMetrics();
      setTableData(data);
    }
    void getData();
  }, []);

  return (
    <Container className="homepage-container text-center">
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
      <Row>
        <Col>
          <Card className="section-card">
            <Card.Body>
              <h1>Tables</h1>
              {Object.keys(tableData).length === 0 ? (
                <p>No table data available.</p>
              ) : (
                <TablesOverviewTable tableData={tableData} />
              )}
            </Card.Body>
          </Card>
        </Col>
      </Row>
    </Container>
  );
}

export default HomePage;