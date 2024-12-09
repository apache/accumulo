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
import './App.css';
import { Routes, Route, Link } from 'react-router-dom';
import { Navbar, Nav, Container, NavDropdown } from 'react-bootstrap';
import HomePage from './components/HomePage';
import ServerOverviewPage from './components/ServerOverviewPage';
import ServerPage from './components/ServerPage';
import ResourceGroupPage from './components/ResourceGroupPage';
import { InstanceMetrics, ServerType } from './types';
import accumuloAvatar from './assets/accumulo-avatar.png';
import { useEffect, useState } from 'react';
import { fetchInstanceMetrics } from './api';
import ResourceGroupsOverviewPage from './components/ResourceGroupsOverviewPage';
import TablesPage from './components/TablesPage';
import TablePage from './components/TablePage';

function App() {
  const [instanceName, setInstanceName] = useState<string>('');

  useEffect(() => {
    async function fetchAndStoreInstanceMetrics() {
      const data: InstanceMetrics = await fetchInstanceMetrics();
      setInstanceName(data.instanceName);
    }
    void fetchAndStoreInstanceMetrics();
  }, []);

  return (
    <>
      <Navbar bg="dark" variant="dark" expand="lg">
        <Container fluid>
          <Navbar.Brand as={Link} to="/" style={{ textDecoration: 'none' }}>
            <img
              src={accumuloAvatar}
              alt="accumulo"
              className="navbar-left"
              id="accumulo-avatar"
              style={{ marginRight: '10px' }}
            />
            {instanceName}
          </Navbar.Brand>
          <Navbar.Toggle aria-controls="basic-navbar-nav" />
          <Navbar.Collapse id="basic-navbar-nav">
            <Nav className="ms-auto">
              <Nav.Link as={Link} to="/resource-groups">
                Resource Groups
              </Nav.Link>
              <NavDropdown title="Servers" id="servers-dropdown">
                <NavDropdown.Item as={Link} to={`/servers/${ServerType.TABLET_SERVER}`}>
                  Tablet Servers
                </NavDropdown.Item>
                <NavDropdown.Item as={Link} to={`/servers/${ServerType.SCAN_SERVER}`}>
                  Scan Servers
                </NavDropdown.Item>
                <NavDropdown.Item as={Link} to={`/server/GARBAGE_COLLECTOR`}>
                  Garbage Collector
                </NavDropdown.Item>
                <NavDropdown.Item as={Link} to={`/server/MANAGER`}>
                  Manager
                </NavDropdown.Item>
                <NavDropdown.Item as={Link} to={`/servers/${ServerType.COMPACTOR}`}>
                  Compactors
                </NavDropdown.Item>
              </NavDropdown>
              <Nav.Link as={Link} to="/tables">
                Tables
              </Nav.Link>
            </Nav>
          </Navbar.Collapse>
        </Container>
      </Navbar>
      <Routes>
        <Route path="/" element={<HomePage />} />
        <Route path="/resource-groups/:rgName" element={<ResourceGroupPage />} />
        <Route path="/servers" element={<ServerOverviewPage />} />
        <Route path="/servers/:serverType" element={<ServerOverviewPage />} />
        <Route path="/server/:serverId" element={<ServerPage />} />
        <Route path="/tables" element={<TablesPage />} />
        <Route path="/resource-groups" element={<ResourceGroupsOverviewPage />} />
        <Route path="/table/:tableName" element={<TablePage />} />
      </Routes>
    </>
  );
}

export default App;
