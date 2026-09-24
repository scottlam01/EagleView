import styles from './TableOfContents.module.css';

interface TableOfContentsProps {
  onSectionClick: (id: string) => void;
}

const TableOfContents = ({
  onSectionClick,
}: TableOfContentsProps) => {
  return (
    <nav className={styles.content}>
      <h2>Table of Contents</h2>

      <ul>
        <li>
          <button onClick={() => onSectionClick("1-data") }>
            1. Data
          </button>
        </li>
        <li>
          <button onClick={() => onSectionClick("2-preprocessing") }>
            2. Preprocessing
          </button>
        </li>
        <li>
          <button onClick={() => onSectionClick("3-data-processing--normalization") }>
            3. Data Processing & Normalization
          </button>
        </li>
        <li>
          <button onClick={() => onSectionClick("4-score-methodology")}>
            4. Score Methodology
          </button>
        </li>
        <li>
          <button onClick={() => onSectionClick("5-dashboard-statistics")}>
            5. Dashboard Statistics
          </button>
        </li>
        <li>
          <button onClick={() => onSectionClick("6-salary-vs-demand-chart-methodology")}>
            6. Salary vs. Demand Chart Methodology
          </button>
        </li>
        <li>
          <button onClick={() => onSectionClick("7-ranking-methodology")}>
            7. Ranking Methodology
          </button>
        </li>
        <li>
          <button onClick={() => onSectionClick("8-interpretation--limitations")}>
            8. Interpretation / Limitations
          </button>
        </li>
      </ul>
    </nav>
  );
};

export default TableOfContents;