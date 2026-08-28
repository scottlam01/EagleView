import Card from "../ui/Card";
import styles from "./ScoreCard.module.css";
import { type DashboardData } from "../../pages/Dashboard";
import { useEffect, useState } from 'react';
import RankingChart from "./RankingChart";

type ScoreCardProps = {
  dashboardData: DashboardData;
};

export default function ScoreCard({
  dashboardData,
}: ScoreCardProps) {

const tabs = ["Demand", "Salary", "Cost"] as const;
const [selectedTab, setSelectedTab] = useState<
  "Demand" | "Salary" | "Cost"
>("Demand");

  return (
  <Card className={styles.scoreCard}>
    <div className={styles.tabsContainer}>
      {tabs.map((tab) => (
        <button
          key={tab}
          className={
            selectedTab === tab
              ? styles.activeTab
              : styles.tab
          }
          onClick={() => setSelectedTab(tab)}
        >
          {tab}
        </button>
      ))}
    </div>

    {selectedTab === "Demand" && (
      <div className={styles.scoreCardContent}>
        <h1>Demand Data</h1>
        <div>Demand Score: {dashboardData.scores.demand_score}</div>
        <div>Outperforms {dashboardData.scores.demand_percentile}% of metros</div>
        <RankingChart dashboardData={dashboardData} scoreType={"demand_score"} ></RankingChart>
      </div>
    )}

    {selectedTab === "Salary" && (
      <div className={styles.scoreCardContent}>
        <h1>Salary Data</h1>
        <div>Salary Score: {dashboardData.scores.salary_score}</div>
        <div>Outperforms {dashboardData.scores.salary_percentile}% of metros</div>
        <RankingChart dashboardData={dashboardData} scoreType={"salary_score"} ></RankingChart>
      </div>
    )}

    {selectedTab === "Cost" && (
      <div className={styles.scoreCardContent}>
        <h1>Cost Data</h1>
        <div>Cost Score: {dashboardData.scores.cost_score}</div>
        <div>Outperforms {dashboardData.scores.cost_percentile}% of metros</div>
        <RankingChart dashboardData={dashboardData} scoreType={"cost_score"} ></RankingChart>
      </div>
    )}

    
  </Card>);
}