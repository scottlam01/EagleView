import { type DashboardData} from "../../pages/Dashboard";

type RankingChartProps = {
  dashboardData: DashboardData;
  scoreType: 
  | "demand_score"
  | "salary_score"
  | "cost_score";
};

export default function RankingChart(
  {dashboardData, 
    scoreType,
  }: RankingChartProps) {

  // creates a copy of all_cbsa and sorts by score (exclude null)
  const rankedCbsa = dashboardData.all_cbsa
  .filter(cbsa => cbsa[scoreType] !== null)
  .sort(
    (a, b) => b[scoreType] - a[scoreType]
  );

  // count all values that are null
  const missingCount = dashboardData.all_cbsa.filter(
    cbsa => cbsa[scoreType] === null
  ).length;

  // create lists of top and bottom 5 ranked areas
  const topFive = rankedCbsa.slice(0, 5);
  const bottomFive = rankedCbsa.slice(-5);

  // get current rank of cbsa
  const currentRank = rankedCbsa.findIndex(
    cbsa => cbsa.cbsa_code === dashboardData.cur_cbsa.cbsa_code
  ) + 1;

  // get cbsa rank
  function getRank(cbsaCode: number) {
    return (
      rankedCbsa.findIndex(
        cbsa => cbsa.cbsa_code === cbsaCode
      ) + 1
    );
  }

  console.log(rankedCbsa);
  
  return (
    <div>
      
      <h3>Top 5</h3>
      
      {topFive.map((cbsa) => (
        <div key={cbsa.cbsa_code}>
          {getRank(cbsa.cbsa_code)}. {cbsa.area_title}: {cbsa[scoreType]}
        </div>
      ))}

      <h3>Your Location</h3>
      {dashboardData.cur_cbsa && (
        <div>
          {getRank(dashboardData.cur_cbsa.cbsa_code)}. {dashboardData.cur_cbsa.area_title}: {dashboardData.scores[scoreType]}
        </div>
      )}

      <h3>Bottom 5</h3>

      {bottomFive.map((cbsa) => (
        <div key={cbsa.cbsa_code}>
          {getRank(cbsa.cbsa_code)}. {cbsa.area_title}: {cbsa[scoreType]}
        </div>
      ))}

      <div>Missing: {missingCount}</div>
    </div>
  )
}
