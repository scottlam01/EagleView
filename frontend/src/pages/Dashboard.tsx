import Footer from '../components/layout/Footer';
import Header from '../components/layout/Header';
import styles from './Dashboard.module.css';

export function Dashboard() {
  return (
    <div>

      <main className={styles.dashboard}>
        
        <Header />

      </main>

      {/* line break */}
      <hr className = {styles.greyLine}></hr>
    
      {/* Footer */}
      <Footer />
    </div>
  )
}