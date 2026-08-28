import Card from "../ui/Card";
import styles from "./SummaryCard.module.css";
import { type DashboardData } from "../../pages/Dashboard";

type SummaryCardProps = {
  dashboardData: DashboardData;
  occupation: string;
  area: string;
};

// Returns string describing opportunity score
function describeScore (demand_score: number, salary_score: number, cost_score: number): string {
  
  interface DescriptorRange {
    max: number;
    value: string;
  }

  const descriptorRanges: DescriptorRange[] = [
    { max: 20,  value: "Very Low" },
    { max: 40,  value: "Below Average" },
    { max: 60,  value: "Average" },
    { max: 80,  value: "Above Average" },
    { max: 100, value: "Exceptional"},
  ];
  const affordabilityRanges: DescriptorRange[] = [
    { max: 20, value: "Very Expensive" },
    { max: 40, value: "Expensive" },
    { max: 60, value: "Average" },
    { max: 80, value: "Affordable" },
    { max: 100, value: "Very Affordable" },
  ];
  
  // get word to describe each score
  const demand_descriptor = descriptorRanges.find(range => demand_score <= range.max)?.value ?? "n/a";
  const salary__descriptor = descriptorRanges.find(range => salary_score <= range.max)?.value ?? "n/a";
  const cost__descriptor = affordabilityRanges.find(range => cost_score <= range.max)?.value ?? "n/a";

  return `${demand_descriptor} Demand, ${salary__descriptor} Salary, ${cost__descriptor}`;
}

export default function SummaryCard({
  dashboardData,
  occupation,
  area
}: SummaryCardProps) {

  console.log(describeScore(dashboardData.scores.demand_score, dashboardData.scores.salary_score, dashboardData.scores.cost_score));

  return (
    <Card>
      <h2>Occupation: {occupation}</h2>
      <p>CBSA: {area}</p>
      <p>Opportunity Score: {dashboardData.scores.opportunity_score}</p>
      <p>{describeScore(dashboardData.scores.demand_score, dashboardData.scores.salary_score, dashboardData.scores.cost_score)}</p>
    </Card>
  );
}