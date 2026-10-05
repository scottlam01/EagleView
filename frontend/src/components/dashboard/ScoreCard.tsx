import Card from "../ui/Card";
import styles from "./ScoreCard.module.css";
import { type DashboardData } from "../../pages/Dashboard";
import { useState } from 'react';
import RankingChart from "./RankingChart";
import { Button, SegmentedControl } from '@mantine/core';
import classes from './ScoreCard.module.css';

type ScoreCardProps = {
  dashboardData: DashboardData;
};

type Tab = "Demand" | "Salary" | "Cost";

export default function ScoreCard({
  dashboardData,
}: ScoreCardProps) {

  const tabs: Tab[] = ["Demand", "Salary", "Cost"];
  const [selectedTab, setSelectedTab] = useState<
    "Demand" | "Salary" | "Cost"
  >("Demand");

  // check if score is available for the selected tab
  const isScoreAvailable =
  selectedTab === "Demand"
    ? dashboardData.scores.demand_score !== null
    : selectedTab === "Salary"
      ? dashboardData.scores.salary_score !== null
      : dashboardData.scores.cost_score !== null;

  return (
  <Card className={styles.scoreCard}>
    <div className={styles.tabsContainer}>
      <SegmentedControl
        radius="xl"
        size="md"
        data={tabs}
        value={selectedTab}
        onChange={setSelectedTab}
        classNames={classes}
        styles={{
          root: {
            backgroundColor: 'white',
          },
        }}
      />
      <Button
        disabled={!isScoreAvailable}
        size="md"
        radius="xl"
        variant="outline" 
        color="gray"
        className={styles.findAreaButton}
        onClick={() => {
          document.getElementById('current-cbsa')?.scrollIntoView({
            behavior: 'smooth',
            block: 'center',
          });
        }}
        styles={{
          root: {
            borderColor: 'var(--mantine-color-gray-1)',
            color: 'var(--mantine-color-dark-6)',
            boxShadow: 'var(--mantine-shadow-md)',
          },
        }}
      >
        Find My Area
      </Button>
    </div>
    

    {selectedTab === "Demand" && (
      <div className={styles.scoreCardContent}>
        <div className={styles.areaTitle}>
          {dashboardData.cur_cbsa.area_title}
        </div>
        <div>
          {dashboardData.scores.demand_percentile !== null
          ? `Outperforms ${dashboardData.scores.demand_percentile}% of metros — Score: ${dashboardData.scores.demand_score}`
          : "Score unavailable — required data is missing for this metro area."}
        </div>
        <RankingChart dashboardData={dashboardData} scoreType={"demand_score"} ></RankingChart>
      </div>
    )}

    {selectedTab === "Salary" && (
      <div className={styles.scoreCardContent}>
        <div className={styles.areaTitle}>
          {dashboardData.cur_cbsa.area_title}
        </div>
        <div>
          {dashboardData.scores.salary_percentile !== null
          ? `Outperforms ${dashboardData.scores.salary_percentile}% of metros — Score: ${dashboardData.scores.salary_score}`
          : "Score unavailable — required data is missing for this metro area."}
        </div>
        <RankingChart dashboardData={dashboardData} scoreType={"salary_score"} ></RankingChart>
      </div>
    )}

    {selectedTab === "Cost" && (
      <div className={styles.scoreCardContent}>
        <div className={styles.areaTitle}>
          {dashboardData.cur_cbsa.area_title}
        </div>
        <div>
          {dashboardData.scores.cost_percentile !== null
          ? `Outperforms ${dashboardData.scores.cost_percentile}% of metros — Score: ${dashboardData.scores.cost_score}`
          : "Score unavailable — required data is missing for this metro area."}
        </div>
        <RankingChart dashboardData={dashboardData} scoreType={"cost_score"} ></RankingChart>
      </div>
    )}

    
  </Card>);
}