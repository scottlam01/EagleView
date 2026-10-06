import logo from '../assets/logo_updated.png';
import Footer from '../components/layout/Footer';
import Searchbar from '../components/search/Searchbar';
import styles from './Home.module.css';

export function Home() {
  return(
    <div>

      {/* Container for logo and searchbar */}
      <section className={styles.hero}>
        
        <img src={logo} className={styles.logo} alt="Logo" />

        <Searchbar />

        <p className={styles.initialSearchNote}>
        The first search may take up to a minute while the server wakes up.
        Subsequent searches should be faster.
        </p>

      </section>

      {/* Footer */}
      <Footer />

    </div>
  )
}