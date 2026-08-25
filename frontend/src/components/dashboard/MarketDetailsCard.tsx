import { type DashboardData} from "../../pages/Dashboard";
import Card from "../ui/Card";
import styles from "./MarketDetailsCard.module.css";
import { useState } from "react";

type MarketDetailsCardProps = {
  dashboardData: DashboardData;
}

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

export default function MarketDetailsCard ({dashboardData,
  }: MarketDetailsCardProps) 
  {
    const [isMarketDetailsOpen, setIsMarketDetailsOpen] = useState(false); // used for collapsible details
    
    return(
      <Card>
        <span className={styles.row}>
          <div className = {styles.marketDetailsHeader} 
              onClick={() => setIsMarketDetailsOpen(!isMarketDetailsOpen)}>
              <div className={styles.chevron}>
                {isMarketDetailsOpen ? "▲" : "▼"}
              </div>
                Market Details
          </div>
        </span>

        {isMarketDetailsOpen && (
          <div className={styles.marketDetailsContent}>
            <h2>Salary</h2>
              <span className={styles.row}>
                <div className={styles.value}>{formatCurrency(dashboardData.cur_cbsa.a_median)}</div>
                <div className={styles.label}>Median Salary</div>
              </span>
              <span className={styles.row}>
                <div className={styles.value}>{formatCurrency(dashboardData.cur_cbsa.a_pct25)}</div>
                <div className={styles.label}>25th Percentile</div>
              </span>
              <span className={styles.row}>
                <div className={styles.value}>{formatCurrency(dashboardData.cur_cbsa.a_pct75)}</div>
                <div className={styles.label}>75th Percentile</div>
              </span>
              <span className={styles.row}>
                <div className={styles.value}>{formatCurrency(dashboardData.cur_cbsa.real_salary)}</div>
                <div className={styles.label}>Real Salary</div>
              </span>
              <span className={styles.row}>
                <div className={styles.value}>{formatCurrency(dashboardData.cur_cbsa.h_median)}</div>
                <div className={styles.label}>Median Hourly Wage</div>
              </span>
            <h2>Demand</h2>
              <span className={styles.row}>
                <div className={styles.value}>{dashboardData.cur_cbsa.tot_emp}</div>
                <div className={styles.label}>Total Employment</div>
              </span>
              <span className={styles.row}>
                <div className={styles.value}>{dashboardData.cur_cbsa.jobs_1000}</div>
                <div className={styles.label}>Jobs / 1,000</div>
              </span>
              <span className={styles.row}>
                <div className={styles.value}>{dashboardData.cur_cbsa.loc_quotient}</div>
                <div className={styles.label}>Location Quotient</div>
              </span>
            <h2>Cost of Living</h2>
              <span className={styles.row}>
                <div className={styles.value}>{dashboardData.cur_cbsa.rpp_all}</div>
                <div className={styles.label}>Overall RPP</div>
              </span>
              <span className={styles.row}>
                <div className={styles.value}>{dashboardData.cur_cbsa.rpp_housing}</div>
                <div className={styles.label}>Housing RPP</div>
              </span>
          </div>
        )}
      </Card>
    )
  }