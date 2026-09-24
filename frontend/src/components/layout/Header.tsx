import styles from './Header.module.css';
import logo from '../../assets/logo_crop.png';
import Searchbar from '../../components/search/Searchbar';
import { useNavigate } from "react-router-dom";

export default function Header () {

  const navigate = useNavigate();

  const handleLogoClick = () => {
    navigate("/");
  };

  return (
    <div>

      {/* header content */}
      <header className = {styles.header}>
        <img onClick = {handleLogoClick} src={logo} className={styles.logo} alt="Logo" />

        <Searchbar/>

        <button className={styles.methodologyButton}
          onClick={() => navigate("/methodology")}>
          Methodology
        </button>
      </header>

      {/* line break */}
        <hr className = {styles.greyLine}></hr>

    </div>
  )
}