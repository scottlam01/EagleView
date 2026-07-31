import Card from "../ui/Card";
import styles from "./HighlightsCard.module.css";
import { type DashboardData } from "../../pages/Dashboard";

type HighlightsCardProps = {
  dashboardData: DashboardData;
};

// formats money amounts, returns N/A if null
export function formatCurrency(value: number | null): string {
  if (value === null) {
    return "N/A";
  }

  return value.toLocaleString("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 2,
  });
}

export default function HighlightsCard({
  dashboardData,
}: HighlightsCardProps){

  return (
    <Card className={styles.highlightsCard}>
      <p className={styles.highlight}>Median Salary<br/>{formatCurrency(dashboardData.cur_cbsa.a_median)}
        <span className={styles.tooltip}>
          Typical annual salary for this occupation in this metro area.
        </span>
      </p>

      <p className={styles.highlight}>Real Salary<br/>{formatCurrency(dashboardData.cur_cbsa.real_salary)}
        <span className={styles.tooltip}>
          Adjusted annual median salary based on local cost of living. Calculated as median salary divided by the Cost Index (RPP ÷ 100) to estimate purchasing power in this metro area.
        </span>
      </p>

      <p className={styles.highlight}>Total Employment<br/>{dashboardData.cur_cbsa.tot_emp} jobs
        <span className={styles.tooltip}>
          Total number of people currently employed in this occupation within this metro area.
        </span>
      </p>

      <p className={styles.highlight}>Job Density<br/>{dashboardData.cur_cbsa.jobs_1000}  jobs per 1,000
        <span className={styles.tooltip}>
          Number of workers in this occupation per 1,000 jobs in the local economy. Higher values mean the occupation is more concentrated here.
        </span>
      </p>

      <p className={styles.highlight}>Cost Index<br/>{dashboardData.cur_cbsa.rpp_all}
        <span className={styles.tooltip}>
          Measures local prices compared to the U.S. average. A value of 100 represents the national average. Values lower than 100 are less than national average, values higher than 100 are higher than national average.
        </span>
      </p>
    </Card>
  );

}