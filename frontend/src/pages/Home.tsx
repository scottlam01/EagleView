import logo from '../assets/logo.png';
import Footer from '../components/layout/Footer';
import styles from './Home.module.css';

export function Home() {
  return(
    <main>

      {/* Container for logo and searchbar */}
      <section className={styles.hero}>
        <div>
          <img src={logo} className={styles.logo} alt="Logo" />
        </div>
        
      </section>

    <hr className = {styles.greyLine}></hr>

    <Footer />

    </main>
  )
}