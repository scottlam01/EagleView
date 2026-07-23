import Card from "../ui/Card";
import styles from "./SummaryCard.module.css";

type SummaryCardProps = {
  occupation: string;
  area: string;
  opportunity_score: number;
  demand_score: number;
  salary_score: number;
  cost_score: number;
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
    { max: Infinity, value: "80 and up" } // Catch-all for everything else
  ];
  
  // get word to describe each score
  const demand_descriptor = descriptorRanges.find(range => demand_score < range.max)?.value ?? "n/a";
  const salary__descriptor = descriptorRanges.find(range => salary_score < range.max)?.value ?? "n/a";
  const cost__descriptor = descriptorRanges.find(range => cost_score < range.max)?.value ?? "n/a";

  return `${demand_descriptor} Demand, ${salary__descriptor} Salary, ${cost__descriptor} Affordability`;
}

export default function SummaryCard({
  occupation,
  area,
  opportunity_score,
  demand_score,
  salary_score,
  cost_score
}: SummaryCardProps) {

  console.log(describeScore(demand_score, salary_score, cost_score));

  return (
    <Card>
      <h2>Occupation: {occupation}</h2>
      <p>CBSA: {area}</p>
      <p>Opportunity Score: {opportunity_score}</p>
      <p>{describeScore(demand_score, salary_score, cost_score)}</p>
    </Card>
  );
}