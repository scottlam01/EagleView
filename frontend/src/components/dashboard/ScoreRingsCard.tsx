import Card from "../ui/Card";
import styles from "./ScoreRingsCard.module.css";
import { type DashboardData } from "../../pages/Dashboard";
import { Center, RingProgress, Tooltip } from '@mantine/core';

type ScoreRingsCardProps = {
  dashboardData: DashboardData;
};

export default function ScoreRingsCard({
  dashboardData,
}: ScoreRingsCardProps) {

  function getScoreColor(score: number | null): string {
    if (score === null) {
      return "gray";
    }

    if (score < 25) {
      return "red";
    }

    if (score < 50) {
      return "orange";
    }

    if (score < 75) {
      return "yellow";
    }

    return "green";
  }

  return (
    <Card className={styles.ringsCard}>
      <div className={styles.ringsContainer}>

        <div className = {styles.smallRingsContainer}> 
          {/* Demand Score Ring */}
          <div className={styles.smallRingContainer}>
            <Tooltip
              label={
                dashboardData.scores.demand_percentile !== null
                  ? "Percentile ranking of this area's Demand Score compared to other metro areas."
                  : "Demand percentile unavailable because one or more demand metrics are missing."
              }
              withArrow
              styles={{
                tooltip: {
                  fontSize: "105%",
                  width: 280,
                  backgroundColor: "#FFFFFF",
                  color: "#1E293B",
                  border: "1px solid #E2E8F0",
                  whiteSpace: "normal",
                  overflow: "visible",
                  textAlign: "center",
                  padding: "8px 10px",
                },
                arrow: {
                  backgroundColor: "#FFFFFF",
                  border: "1px solid #E2E8F0",
                },
              }}
            >
              <RingProgress
                size={75}
                roundCaps
                thickness={6}
                sections={[{
                  value: dashboardData.scores.demand_percentile ?? 0,
                  color: getScoreColor(dashboardData.scores.demand_percentile)
                }]}
                label={
                  <Center fw={900}>
                    {dashboardData.scores.demand_percentile !== null
                    ? `${dashboardData.scores.demand_percentile.toFixed(0)}%`
                    : "N/A"}
                  </Center>
                }
              />
            </Tooltip>
            <div className={styles.ringName}>
              <h1>
                DEMAND
              </h1>
            </div>
          </div>

          {/* Salary Score Ring */}
          <div className={styles.smallRingContainer}>
            <Tooltip
              label={
                dashboardData.scores.salary_percentile !== null
                  ? "Percentile ranking of this area's Salary Score compared to other metro areas."
                  : "Salary percentile unavailable because one or more salary metrics are missing."
              }
              withArrow
              styles={{
                tooltip: {
                  fontSize: "105%",
                  width: 280,
                  backgroundColor: "#FFFFFF",
                  color: "#1E293B",
                  border: "1px solid #E2E8F0",
                  whiteSpace: "normal",
                  overflow: "visible",
                  textAlign: "center",
                  padding: "8px 10px",
                },
                arrow: {
                  backgroundColor: "#FFFFFF",
                  border: "1px solid #E2E8F0",
                },
              }}
            >
              <RingProgress
                size={75}
                roundCaps
                thickness={6}
                sections={[{
                  value: dashboardData.scores.salary_percentile ?? 0,
                  color: getScoreColor(dashboardData.scores.salary_percentile)
                }]}
                label={
                  <Center fw={900}>
                    {dashboardData.scores.salary_percentile !== null
                    ? `${dashboardData.scores.salary_percentile.toFixed(0)}%`
                    : "N/A"}
                  </Center>
                }
              />
            </Tooltip>
            <div className={styles.ringName}>
              <h1>
                SALARY
              </h1>
            </div>
          </div>

          {/* Cost Score Ring */}
          <div className={styles.smallRingContainer}>
            <Tooltip
              label={
                dashboardData.scores.cost_percentile !== null
                  ? "Percentile ranking of this area's Cost Score compared to other metro areas."
                  : "Cost percentile unavailable because cost-of-living data is missing."
              }
              withArrow
              styles={{
                tooltip: {
                  fontSize: "105%",
                  width: 280,
                  backgroundColor: "#FFFFFF",
                  color: "#1E293B",
                  border: "1px solid #E2E8F0",
                  whiteSpace: "normal",
                  overflow: "visible",
                  textAlign: "center",
                  padding: "8px 10px",
                },
                arrow: {
                  backgroundColor: "#FFFFFF",
                  border: "1px solid #E2E8F0",
                },
              }}
            >
              <RingProgress
                size={75}
                roundCaps
                thickness={6}
                sections={[{
                  value: dashboardData.scores.cost_percentile ?? 0,
                  color: getScoreColor(dashboardData.scores.cost_percentile)
                }]}
                label={
                  <Center fw={900}>
                    {dashboardData.scores.cost_percentile !== null
                    ? `${dashboardData.scores.cost_percentile.toFixed(0)}%`
                    : "N/A"}
                  </Center>
                }
              />
            </Tooltip>
            <div className={styles.ringName}>
              <h1>
                COST
              </h1>
            </div>
          </div>
        </div>

        {/* Opportunity Score Ring */}
        <div className={styles.opportunityContainer}>

          <Tooltip
            label={
              dashboardData.scores.opportunity_percentile !== null
                ? "Percentile ranking of this area's overall Opportunity Score compared to other metro areas."
                : "Opportunity percentile unavailable because one or more required scores are missing."
            }
            withArrow
              styles={{
                tooltip: {
                  fontSize: "105%",
                  width: 280,
                  backgroundColor: "#FFFFFF",
                  color: "#1E293B",
                  border: "1px solid #E2E8F0",
                  whiteSpace: "normal",
                  overflow: "visible",
                  textAlign: "center",
                  padding: "8px 10px",
                },
                arrow: {
                  backgroundColor: "#FFFFFF",
                  border: "1px solid #E2E8F0",
                },
              }}
            >
            <RingProgress
              className={styles.opportunityRing}
              size={110}
              roundCaps
              thickness={10}
              sections={[{
                value: dashboardData.scores.opportunity_percentile ?? 0,
                color: getScoreColor(dashboardData.scores.opportunity_percentile)
              }]}
              label={
                <Center fw={900}>
                  {dashboardData.scores.opportunity_percentile !== null
                  ? `${dashboardData.scores.opportunity_percentile.toFixed(0)}%`
                  : "N/A"}
                </Center>
              }
            />
          </Tooltip>
          
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