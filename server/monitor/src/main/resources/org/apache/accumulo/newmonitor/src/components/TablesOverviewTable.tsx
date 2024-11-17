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
import { Table } from 'react-bootstrap';
import { TablesMetrics } from '../types';
import { Link } from 'react-router-dom';

interface TablesTableProps {
  tableData: TablesMetrics;
}

function TablesOverviewTable({ tableData }: TablesTableProps) {
  return (
    <div className="tables-table-container">
      <Table striped bordered hover size="lg" className="mx-auto">
        <thead>
          <tr>
            <th>Table Name</th>
            <th>Total Entries</th>
            <th>Total Size On Disk</th>
            <th>Total Files</th>
            <th>Total WALs</th>
            <th>Total Tablets</th>
          </tr>
        </thead>
        <tbody>
          {Object.entries(tableData).map(([tableName, metrics]) => (
            <tr key={tableName}>
              <td>
                <Link to={`/table/${tableName}`}>{tableName}</Link>
              </td>
              <td>{metrics.totalEntries}</td>
              <td>{metrics.totalSizeOnDisk}</td>
              <td>{metrics.totalFiles}</td>
              <td>{metrics.totalWals}</td>
              <td>{metrics.totalTablets}</td>
            </tr>
          ))}
        </tbody>
      </Table>
    </div>
  );
}

export default TablesOverviewTable;