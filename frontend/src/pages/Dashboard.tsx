import Footer from '../components/layout/Footer';
import Header from '../components/layout/Header';
import styles from './Dashboard.module.css';
import SummaryCard from '../components/dashboard/SummaryCard';
import { useParams, useLocation } from "react-router-dom";
import { useEffect, useState } from 'react';
import HighlightsCard from '../components/dashboard/HighlightsCard';
export function Dashboard() {

  const [dashboardData, setDashboardData] = useState<DashboardData | null>(null);

  // holds data for current cbsa
  type CurCbsa = {
    cbsa_code: number;
    occ_code: string;

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
  type Scores = {
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
  type AllCbsa = {
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

  // collectively holds all dashboard data
  type DashboardData = {
    cur_cbsa: CurCbsa;
    scores: Scores;
    all_cbsa: AllCbsa[];
  };

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

  return (
    <div className={styles.page}>

      <Header />

      <main className={styles.dashboard}>
        <SummaryCard
        occupation={occupation}
        area={area}
        opportunity_score={dashboardData.scores.opportunity_score}
        demand_score={dashboardData.scores.demand_score}
        salary_score={dashboardData.scores.salary_score}
        cost_score={dashboardData.scores.cost_score}/>

        <div className={styles.HighlightsCardContainer}>
          <HighlightsCard 
          a_median={dashboardData.cur_cbsa.a_median}
          real_salary={dashboardData.cur_cbsa.real_salary} 
          tot_emp={dashboardData.cur_cbsa.tot_emp}
          jobs_1000={dashboardData.cur_cbsa.jobs_1000}
          rpp_all={dashboardData.cur_cbsa.rpp_all}>
          </HighlightsCard>
        </div>
      </main>
    
      {/* Footer */}
      <Footer />
    </div>
  )
}