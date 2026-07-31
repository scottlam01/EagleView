import Footer from '../components/layout/Footer';
import Header from '../components/layout/Header';
import styles from './Dashboard.module.css';
import SummaryCard from '../components/dashboard/SummaryCard';
import { useParams, useLocation } from "react-router-dom";
import { useEffect, useState } from 'react';
import HighlightsCard from '../components/dashboard/HighlightsCard';
import ScoreCard from '../components/dashboard/ScoreCard';

// collectively holds all dashboard data
export type DashboardData = {
  cur_cbsa: CurCbsa;
  scores: Scores;
  all_cbsa: AllCbsa[];
};

// holds data for current cbsa
export type CurCbsa = {
  cbsa_code: number;
  occ_code: string;
  occ_title: string;
  area_title: string;
  tot_emp: number;
  jobs_1000: number;
  loc_quotient: number;

  h_median: number;
  a_pct25: number;
  a_median: number;
  a_pct75: number;

  real_salary: number | null;

  rpp_all: number;
  rpp_housing: number;
};

// holds scores for current cbsa
export type Scores = {
  demand_score: number;
  demand_percentile: number;

  salary_score: number;
  salary_percentile: number;

  cost_score: number;
  cost_percentile: number;

  opportunity_score: number;
  opportunity_percentile: number;
};

// holds data for all cbsa
  export type AllCbsa = {
    cbsa_code: number;
    area_title: string;

    demand: {
      tot_emp: number;
      jobs_1000: number;
      loc_quotient: number;
    };

    salary: {
      a_median: number;
      a_pct25: number;
      a_pct75: number;
    };

    cost: {
      rpp_all: number;
      rpp_housing: number;
    };

    demand_score: number;
    salary_score: number;
    cost_score: number;
    opportunity_score: number;

    demand_percentile: number;
    salary_percentile: number;
    cost_percentile: number;
    opportunity_percentile: number;
  };

export function Dashboard() {

  const [dashboardData, setDashboardData] = useState<DashboardData | null>(null);

  // grabs occupation and area from state
  const location = useLocation();
  const { occupation, area } = location.state ?? {
    occupation: "",
    area: "",
  };

  // grabs cbsa_code and occ_code from url params
  const { cbsa_code, occ_code } = useParams<{
    cbsa_code: string;
    occ_code: string;
  }>();

  // initial load of dashboard data
  useEffect(() => {
  async function fetchDashboard() {

      if (!cbsa_code || !occ_code) return;

      const response = await fetch(
        `http://localhost:8000/dashboard/${occ_code}/${cbsa_code}`
      );

      const data = await response.json();

      setDashboardData(data);
    }

    fetchDashboard();

  }, [cbsa_code, occ_code]);

  if (!dashboardData) {
    return <div>Loading...</div>;
  }

  console.log(dashboardData);
  // returns all demand, salary, or cost scores as dictionary with pairs {cbsa: score}

  return (
    <div className={styles.page}>

      <Header />

      <div className={styles.dashboard}>

        <SummaryCard
        dashboardData={dashboardData}
        occupation={occupation}
        area={area}/>

        <div className={styles.HighlightsCardContainer}>
          <HighlightsCard dashboardData={dashboardData}></HighlightsCard>
        </div>

        <ScoreCard dashboardData={dashboardData}></ScoreCard>
        
      </div>
    
      {/* Footer */}
      <Footer />
    </div>
  )
}