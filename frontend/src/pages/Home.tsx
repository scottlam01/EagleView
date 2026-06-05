import logo from '../assets/logo_crop.png';
import Footer from '../components/layout/Footer';
import Searchbar from '../components/search/Searchbar';
import styles from './Home.module.css';

export function Home() {
  return(
    <main>

      {/* Container for logo and searchbar */}
      <section className={styles.hero}>
        
        <img src={logo} className={styles.logo} alt="Logo" />

        <Searchbar />

      </section>
      
      {/* line break */}
      <hr className = {styles.greyLine}></hr>

      {/* Footer */}
      <Footer />

    </main>
  )
}