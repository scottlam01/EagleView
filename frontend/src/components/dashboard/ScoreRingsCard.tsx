import Card from "../ui/Card";
import styles from "./ScoreRingsCard.module.css";
import { type DashboardData } from "../../pages/Dashboard";
import { Center, RingProgress } from '@mantine/core';

type ScoreRingsCardProps = {
  dashboardData: DashboardData;
};

export default function ScoreRingsCard({
  dashboardData,
}: ScoreRingsCardProps) {

  return (
    <Card className={styles.ringsCard}>
      <div className={styles.ringsContainer}>

        <div className = {styles.smallRingsContainer}> 
          {/* Demand Score Ring */}
          <div className={styles.smallRingContainer}>
            <RingProgress
              size={75}
              roundCaps
              thickness={6}
              sections={[{ value: dashboardData.scores.demand_percentile, color: 'red' }]}
              label={
                <Center fw={900}>
                  {dashboardData.scores.demand_percentile.toFixed(0)}%
                </Center>
              }
            />
            <div className={styles.ringName}>
              <h1>
                DEMAND
              </h1>
            </div>
          </div>

          {/* Salary Score Ring */}
          <div className={styles.smallRingContainer}>
            <RingProgress
              size={75}
              roundCaps
              thickness={6}
              sections={[{ value: dashboardData.scores.salary_percentile, color: 'green' }]}
              label={
                <Center fw={900}>
                  {dashboardData.scores.salary_percentile.toFixed(0)}%
                </Center>
              }
            />
            <div className={styles.ringName}>
              <h1>
                SALARY
              </h1>
            </div>
          </div>

          {/* Cost Score Ring */}
          <div className={styles.smallRingContainer}>
            <RingProgress
              size={75}
              roundCaps
              thickness={6}
              sections={[{ value: dashboardData.scores.cost_percentile, color: 'blue' }]}
              label={
                <Center fw={900}>
                  {dashboardData.scores.cost_percentile.toFixed(0)}%
                </Center>
              }
            />
            <div className={styles.ringName}>
              <h1>
                COST
              </h1>
            </div>
          </div>
        </div>

        {/* Opportunity Score Ring */}
        <div className={styles.opportunityContainer}>
          <RingProgress
            className={styles.opportunityRing}
            size={110}
            roundCaps
            thickness={10}
            sections={[{ value: dashboardData.scores.opportunity_percentile, color: 'purple' }]}
            label={
              <Center fw={900}>
                {dashboardData.scores.opportunity_percentile.toFixed(0)}%
              </Center>
            }
          />
          <div className={styles.ringName}>
            <h1 style={{ textAlign: "center" }}>
              OPPORTUNITY PERCENTILE
            </h1>
          </div>
        </div>

        
        

      </div>
    </Card> 
  );
}