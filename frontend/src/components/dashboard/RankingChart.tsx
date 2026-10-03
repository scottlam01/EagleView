import { type DashboardData} from "../../pages/Dashboard";
import styles from "./RankingChart.module.css";
import { useState } from 'react';
import { IconChevronDown, IconChevronUp, IconSearch, IconSelector } from '@tabler/icons-react';
import {
  Center,
  Group,
  ScrollArea,
  Table,
  Text,
  TextInput,
  UnstyledButton,
} from '@mantine/core';
import classes from './RankingChart.module.css';

type RankingChartProps = {
  dashboardData: DashboardData;
  scoreType: 
  | "demand_score"
  | "salary_score"
  | "cost_score";
};

interface RowData {
  rank: number;
  cbsa_code: number;
  cbsa: string;
  score: number;
}

interface ThProps {
  children: React.ReactNode;
  reversed: boolean;
  sorted: boolean;
  onSort: () => void;
  style?: React.CSSProperties;
}

function Th({ children, reversed, sorted, onSort, style }: ThProps) {
  const Icon = sorted ? (reversed ? IconChevronUp : IconChevronDown) : IconSelector;
  return (
    <Table.Th className={classes.th} style={style}>
      <UnstyledButton onClick={onSort} className={classes.control}>
        <Group justify="space-between">
          <Text fw={500} fz="sm">
            {children}
          </Text>
          <Center className={classes.icon}>
            <Icon size={16} stroke={1.5} />
          </Center>
        </Group>
      </UnstyledButton>
    </Table.Th>
  );
}

// search function to filter data based on cbsa name
function filterData(data: RowData[], search: string) {
  const query = search.toLowerCase().trim();
  return data.filter((item) =>
    item.cbsa.toLowerCase().includes(query)
  );
}

// sort function to sort data based on score or cbsa name
function sortData(
  data: RowData[],
  payload: {
    sortBy: keyof RowData | null;
    reversed: boolean;
    search: string;
  }
) {
  const { sortBy } = payload;

  if (!sortBy) {
    return filterData(data, payload.search);
  }

  return filterData(
    [...data].sort((a, b) => {
      const aValue = a[sortBy];
      const bValue = b[sortBy];

      // Sort numbers numerically.
      if (typeof aValue === 'number' && typeof bValue === 'number') {
        return payload.reversed
          ? bValue - aValue
          : aValue - bValue;
      }

      // Sort strings alphabetically.
      if (typeof aValue === 'string' && typeof bValue === 'string') {
        return payload.reversed
          ? bValue.localeCompare(aValue)
          : aValue.localeCompare(bValue);
      }

      return 0;
    }),
    payload.search
  );
}


export default function RankingChart(
  {dashboardData, 
    scoreType,
  }: RankingChartProps) {

  const [search, setSearch] = useState('');
  const [sortBy, setSortBy] = useState<keyof RowData | null>(null);
  const [reverseSortDirection, setReverseSortDirection] = useState(false);

  // filter out missing scores, sort by score, then convert to table row format.
  const data: RowData[] = dashboardData.all_cbsa
    .filter((cbsa) => cbsa[scoreType] !== null)
    .sort((a, b) => b[scoreType] - a[scoreType])
    .map((cbsa, index) => ({
      rank: index + 1,
      cbsa_code: cbsa.cbsa_code,
      cbsa: `${cbsa.area_title}`,
      score: cbsa[scoreType],
    }));
  const [sortedData, setSortedData] = useState(data); // have to put after creating data

  const setSorting = (field: keyof RowData) => {
    const reversed = field === sortBy ? !reverseSortDirection : false;
    setReverseSortDirection(reversed);
    setSortBy(field);
    setSortedData(sortData(data, { sortBy: field, reversed, search }));
  };
  
  const handleSearchChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    const { value } = event.currentTarget;
    setSearch(value);
    setSortedData(sortData(data, { sortBy, reversed: reverseSortDirection, search: value }));
  };

  const rows = sortedData.map((row) => (
    <Table.Tr 
    key={row.cbsa_code}
    id={row.cbsa_code === dashboardData.cur_cbsa.cbsa_code ? 'current-cbsa' : undefined}
    style={{
      backgroundColor: row.cbsa_code === dashboardData.cur_cbsa.cbsa_code ? '#fef3c7' : 'transparent',
    }}>
      <Table.Td>{row.rank}</Table.Td>
      <Table.Td>{row.cbsa}</Table.Td>
      <Table.Td>{row.score}</Table.Td>
    </Table.Tr>
  ));
  
  return (
    <ScrollArea>
      <TextInput
        placeholder="Search by CBSA"
        mb="md"
        leftSection={<IconSearch size={16} stroke={1.5} />}
        value={search}
        onChange={handleSearchChange}
      />
      <Table horizontalSpacing="md" verticalSpacing="xs" miw={200} layout="fixed">
        <Table.Tbody>
          <Table.Tr>
            <Th
              sorted={sortBy === 'rank'}
              reversed={reverseSortDirection}
              onSort={() => setSorting('rank')}
              style={{ width: '25%' }}
            >
              Rank
            </Th>
            <Th
              sorted={sortBy === 'cbsa'}
              reversed={reverseSortDirection}
              onSort={() => setSorting('cbsa')}
              style={{ width: '50%' }}
            >
              CBSA
            </Th>
            <Th
              sorted={sortBy === 'score'}
              reversed={reverseSortDirection}
              onSort={() => setSorting('score')}
            >
              Score
            </Th>
          </Table.Tr>
        </Table.Tbody>
        <Table.Tbody>
          {rows.length > 0 ? (
            rows
          ) : (
            <Table.Tr>
              <Table.Td colSpan={Object.keys(data[0]).length}>
                <Text fw={500} ta="center">
                  Nothing found
                </Text>
              </Table.Td>
            </Table.Tr>
          )}
        </Table.Tbody>
      </Table>
    </ScrollArea>
  );
}
