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
import { useParams, Link } from 'react-router-dom';
import { fetchTableMetrics, fetchTableTabletInfo } from '../api';
import { TableMetrics, TabletMetrics } from '../types';
import { Table, Row, Col, Container } from 'react-bootstrap';
import StatisticGroup from './StatisticGroup';

function TablePage() {
  const { tableName } = useParams<{ tableName?: string }>();
  const [tableMetrics, setTableMetrics] = useState<TableMetrics | null>(null);
  const [tabletInfo, setTabletInfo] = useState<TabletMetrics[]>([]);

  useEffect(() => {
    async function getData() {
      if (!tableName) {
        console.error('Table name is undefined');
        return;
      }

      try {
        const [metrics, tablets]: [TableMetrics, TabletMetrics[]] = await Promise.all([
          fetchTableMetrics(tableName),
          fetchTableTabletInfo(tableName)
        ]);
        setTableMetrics(metrics);
        setTabletInfo(tablets);
      } catch (error) {
        console.error('Error fetching table data:', error);
      }
    }
    void getData();
  }, [tableName]);

  return (
    <Container className="homepage-container">
      <div>
        <h1>Table: {tableName}</h1>

        {/* Display Table Statistics */}
        {tableMetrics ? (
          <div>
            <h2>Table Statistics</h2>
            <Row>
              <Col md={6} sm={12}>
                <StatisticGroup
                  title="Entries & Size"
                  statistics={[
                    { label: 'Total Entries', value: tableMetrics.totalEntries },
                    { label: 'Total Size On Disk', value: tableMetrics.totalSizeOnDisk },
                  ]}
                />
                <StatisticGroup
                  title="Tablet Assignments"
                  statistics={[
                    { label: 'Total Assigned Tablets', value: tableMetrics.totalAssignedTablets },
                    { label: 'Assigned to Dead Servers', value: tableMetrics.totalAssignedToDeadServerTablets },
                    { label: 'Total Hosted Tablets', value: tableMetrics.totalHostedTablets },
                    { label: 'Total Suspended Tablets', value: tableMetrics.totalSuspendedTablets },
                    { label: 'Total Unassigned Tablets', value: tableMetrics.totalUnassignedTablets },
                  ]}
                />
              </Col>
              <Col md={6} sm={12}>
                <StatisticGroup
                  title="Files & WALs"
                  statistics={[
                    { label: 'Total Files', value: tableMetrics.totalFiles },
                    { label: 'Total WALs', value: tableMetrics.totalWals },
                  ]}
                />
                <StatisticGroup
                  title="Tablet Counts"
                  statistics={[
                    { label: 'Total Tablets', value: tableMetrics.totalTablets },
                    { label: 'Available Always', value: tableMetrics.availableAlways },
                    { label: 'Available On Demand', value: tableMetrics.availableOnDemand },
                    { label: 'Available Never', value: tableMetrics.availableNever },
                  ]}
                />
              </Col>
            </Row>
          </div>
        ) : (
          <p>No table statistics available.</p>
        )}

        {/* Display Tablet Metrics */}
        <h2>Tablet Metrics</h2>
        {tabletInfo.length === 0 ? (
          <p>No metrics found for table: {tableName}</p>
        ) : (
          <Table striped bordered hover size="lg" className="mx-auto">
            <thead>
              <tr>
                <th>Tablet ID</th>
                <th>Number of Files</th>
                <th>Number of WAL Logs</th>
                <th>Estimated Entries</th>
                <th>Tablet State</th>
                <th>Tablet Directory</th>
                <th>Tablet Availability</th>
                <th>Estimated Size</th>
                <th>Location</th>
              </tr>
            </thead>
            <tbody>
              {tabletInfo.map((tablet) => (
                <tr key={tablet.tabletId}>
                  <td>{tablet.tabletId}</td>
                  <td>{tablet.numFiles}</td>
                  <td>{tablet.numWalLogs}</td>
                  <td>{tablet.estimatedEntries}</td>
                  <td>{tablet.tabletState}</td>
                  <td>{tablet.tabletDir}</td>
                  <td>{tablet.tabletAvailability}</td>
                  <td>{tablet.estimatedSize}</td>
                  <td>{tablet.location}</td>
                </tr>
              ))}
            </tbody>
          </Table>
        )}
        <Link to="/tables">Back to Tables</Link>
      </div>
    </Container>
  );
}

export default TablePage;