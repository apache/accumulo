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
import { Card, ListGroup } from 'react-bootstrap';

interface Statistic {
  label: string;
  value: number | string;
}

interface StatisticGroupProps {
  title: string;
  statistics: Statistic[];
}

/**
 * Component that displays a group of statistics within a card.
 *
 * @component
 * @param {Object} props - The component props.
 * @param {string} props.title - The title of the statistic group.
 * @param {Array} props.statistics - An array of statistics to display.
 * @param {string} props.statistics[].label - The label of the statistic.
 * @param {string | number} props.statistics[].value - The value of the statistic.
 * @returns {JSX.Element} A card component containing the statistics.
 */
function StatisticGroup({ title, statistics }: StatisticGroupProps): JSX.Element {
  return (
    <Card className="mb-3">
      <Card.Body>
        <Card.Title>{title}</Card.Title>
        <ListGroup variant="flush">
          {statistics.map((stat) => (
            <ListGroup.Item key={stat.label}>
              <strong>{stat.label}:</strong> {stat.value}
            </ListGroup.Item>
          ))}
        </ListGroup>
      </Card.Body>
    </Card>
  );
}

export default StatisticGroup;