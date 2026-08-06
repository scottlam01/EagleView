import { type DashboardData } from "../../pages/Dashboard";
import Card from "../ui/Card";
import styles from "./PlotlyCard.module.css";
import Plot from "react-plotly.js";

type PlotlyCardProps = {
  dashboardData: DashboardData;
}

export default function PlotlyCard ({dashboardData,
  }: PlotlyCardProps) {

    const chartData = dashboardData.all_cbsa.map((cbsa) => ({
    ...cbsa,}));

    // used to calculate sizing for area bubbles
    const maxEmployment = Math.max(
      ...chartData.map(c => c.demand.tot_emp)
    );
    const sizeref = 2 * maxEmployment / (50 ** 2); // "100" desired maximum bubble diameter in pixels

    // sorted employment values
    const sortedEmployment = chartData
    .map(c => c.demand.tot_emp)
    .sort((a, b) => a - b);

    

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
      <div className={styles.chartContainer}>
        <Plot
          data={[
            {
              x: chartData.map(c => c.demand_score),
              y: chartData.map(c => c.salary_score),
              text: chartData.map(c => c.area_title),
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
            title: {
              text: "Salary vs Demand by CBSA",
              font: {
                size: 20,
              },
              x: 0.5,
              xanchor: "center",
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
