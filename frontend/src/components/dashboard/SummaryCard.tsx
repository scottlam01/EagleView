import Card from "../ui/Card";
import styles from "./SummaryCard.module.css";
import { type DashboardData } from "../../pages/Dashboard";

type SummaryCardProps = {
  dashboardData: DashboardData;
  occupation: string;
  area: string;
};

export default function SummaryCard({
  dashboardData,
  occupation,
  area
}: SummaryCardProps) {

  return (
    <Card className={styles.summaryCard}>
      <div className={styles.title}>
        <h2>{dashboardData.cur_cbsa.occ_title}</h2>
      </div>
      <div className={styles.area}>
        <p>{dashboardData.cur_cbsa.area_title}</p>
      </div>
      <hr></hr>
      <div className={styles.definition}>
        <p>{dashboardData.cur_cbsa.occ_definition}</p>
      </div>
    </Card> 
  );
}