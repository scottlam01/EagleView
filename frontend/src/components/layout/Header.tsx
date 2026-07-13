import styles from './Header.module.css';
import logo from '../../assets/logo_crop.png';
import Searchbar from '../../components/search/Searchbar';

export default function Header () {
  return (
    <div>

      {/* header content */}
      <header className = {styles.header}>
        <img src={logo} className={styles.logo} alt="Logo" />

        <div className = {styles.search}>
          <Searchbar />
        </div>
      </header>

      {/* line break */}
        <hr className = {styles.greyLine}></hr>

    </div>
  )
}