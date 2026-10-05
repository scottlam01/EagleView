import { type DashboardData } from "../../pages/Dashboard";
import Card from "../ui/Card";
import styles from "./PlotlyCard.module.css";
import Plot from "react-plotly.js";
import { useState } from 'react';
import { Menu, UnstyledButton } from "@mantine/core";
import { IconChevronDown } from "@tabler/icons-react";
import classes from './PlotlyCard.module.css';

type PlotlyCardProps = {
  dashboardData: DashboardData;
}

export default function PlotlyCard ({dashboardData,
  }: PlotlyCardProps) {

    const [selectedState, setSelectedState] = useState("ALL");
    const [opened, setOpened] = useState(false);

    // get available states from chartData
    const availableStates = [
      ...new Set(
        dashboardData.all_cbsa.map((cbsa) => cbsa.prim_state)
      ),
    ].sort();

    // chartData
    const chartData =
    selectedState === "ALL"
      ? dashboardData.all_cbsa
      : dashboardData.all_cbsa.filter(
          (cbsa) => cbsa.prim_state === selectedState
        );

    // sorted employment values
    const sortedEmployment = chartData
    .map(c => c.demand.tot_emp)
    .sort((a, b) => a - b);

    // create menu items
    const stateItems = availableStates.map((state) => (
      <Menu.Item
        key={state}
        onClick={() => setSelectedState(state)}
      >
        {state}
      </Menu.Item>
    ));    

    function getEmploymentPercentile(emp: number) {
      const count = sortedEmployment.filter(v => v <= emp).length;

      return (count - 1) / (sortedEmployment.length - 1);
    }
    function getBubbleSize(emp: number) {
      const p = getEmploymentPercentile(emp);

      if (p < 0.30) {
        return 10; // Q1 // 10
      }

      else if (p < 0.50) {
        return 15; // Q2 // 15
      }

      else if (p < 0.90) {
        return 15; // Q3 // 15
      }

      return 30; // Q4 // 50
    }

    return (
    <Card className={styles.chartCard}>

      <div className={styles.chartHeader}>
        
        <Menu
          onOpen={() => setOpened(true)}
          onClose={() => setOpened(false)}
          radius="md"
          width="target"
          withinPortal
        >
          <Menu.Target>
            <UnstyledButton
              className={classes.control}
              data-expanded={opened || undefined}>
              <span>
                {selectedState === "ALL" ? "All States" : selectedState}
              </span>
              <IconChevronDown
                size={16}
                className={styles.icon}
                stroke={1.5}
              />
            </UnstyledButton>
          </Menu.Target>
          <Menu.Dropdown className={styles.dropdown}>
            <Menu.Item onClick={() => setSelectedState("ALL")}>
              All States
            </Menu.Item>
            {stateItems}
          </Menu.Dropdown>
        </Menu>

        <h1 className={styles.chartTitle}>Salary vs Demand</h1>

      </div>

      <div className={styles.chartContainer}>
        <Plot
          data={[
            {
              x: chartData.map(c => c.demand_score),
              y: chartData.map(c => c.salary_score),
              hovertemplate:
                "<span style='font-size: 16px'> %{customdata[0]}</span><br>" +
                "<span style='font-size: 18px'>Opportunity Score: %{customdata[1]}</span><br>" +
                "────────────────────<br>" +
                "<span style='font-size: 14px'>Demand Score: %{x}</span><br>" +
                "<span style='font-size: 14px'>Salary Score: %{customdata[2]}</span><br>" +
                "<span style='font-size: 14px'>Cost Score: %{customdata[3]}</span><br>" +
                "<extra></extra>",
              customdata: chartData.map(c => [
                c.area_title,
                c.opportunity_score,
                c.salary_score,
                c.cost_score,
              ]),
              mode: "markers",
              type: "scatter",
              marker: {
                symbol: chartData.map(c =>
                  c.cbsa_code === dashboardData.cur_cbsa.cbsa_code
                    ? "star"
                    : "circle"
                ),
                
                size: chartData.map(c => getBubbleSize(c.demand.tot_emp)),
                sizemode: "diameter",
                color: chartData.map(c => c.opportunity_score),
                colorscale: [
                  [0, "#d73027"],
                  [0.5, "#fee08b"],
                  [1, "blue"],
                ],
                colorbar: {
                  title: {
                    text: "Opportunity Score",
                    side: "top",
                    font: {
                      size: 14,
                    },
                  }
                },
                showscale: true,
                line: {
                  color: "#555",
                  width: 1,
                },
              },
            },
          ]}
          layout={{
            font: {
              family: "Nunito Sans, sans-serif",
              size: 14,
            },
            xaxis: {
              title: {
                text: "Demand Score",
                font: {
                  size: 16,
                },
              },
            },
            yaxis: {
              title: {
                text: "Salary Score",
                font: {
                  size: 16,
                },
              },
            },
            dragmode: "pan",
          }}
          config={{
            scrollZoom: true,
          }}
          useResizeHandler
          style={{
            width: "100%",
            height: "100%",
          }}
        />
      </div>

      

    </Card>)
}
