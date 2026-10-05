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

// formats numbers, returns N/A if null
export function formatNumber(
  value: number | null,
  decimals: number = 0
): string {
  if (value === null) {
    return "N/A";
  }

  return value.toFixed(decimals);
}

export default function HighlightsCard({
  dashboardData,
}: HighlightsCardProps){

  return (
    <Card className={styles.highlightsCard}>

      <div className={styles.highlight}>
        <p className={styles.number}>{formatCurrency(dashboardData.cur_cbsa.a_median)}</p>
        <p className={styles.title}>Median Salary</p>
        <p className={styles.explanation}>
          Typical annual salary for this occupation in this metro area.
        </p>
      </div>

      <div className={styles.highlight}>
        <p className={styles.number}>{formatCurrency(dashboardData.cur_cbsa.real_salary)}</p>
        <p className={styles.title}>Real Salary</p>
        <p className={styles.explanation}>
          Adjusted median salary based on local cost of living, calculated using the Cost Index.
        </p>
      </div>

      <div className={styles.highlight}>
        <p className={styles.number}>{formatNumber(dashboardData.cur_cbsa.tot_emp)}</p>
        <p className={styles.title}>Total Employment</p>
        <p className={styles.explanation}>
          Total number of people employed in this occupation within this metro area.
        </p>
      </div>

      <div className={styles.highlight}>
        <p className={styles.number}>{formatNumber(dashboardData.cur_cbsa.jobs_1000, 2)}</p>
        <p className={styles.title}>Jobs per 1,000</p>
        <p className={styles.explanation}>
          Number of workers in this occupation per 1,000 jobs in the local economy. Higher values indicate greater concentration in the area.
        </p>
      </div>

      <div className={styles.highlight}>
        <p className={styles.number}>{formatNumber(dashboardData.cur_cbsa.rpp_all, 1)}</p>
        <p className={styles.title}>Cost Index</p>
        <p className={styles.explanation}>
          Local prices compared to the U.S. average. 100 is average; below 100 is lower, above 100 is higher.
        </p>
      </div>

    </Card>
  );

}