import styles from './Header.module.css';
import logo from '../../assets/logo_updated.png';
import Searchbar from '../../components/search/Searchbar';
import { useNavigate } from "react-router-dom";
import { Button } from '@mantine/core';

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

        <Button
          size="md"
          radius="xl"
          variant="outline" 
          color="gray"
          className={styles.methodologyButton}
          onClick={() => navigate("/methodology")}
          styles={{
            root: {
              borderColor: 'var(--mantine-color-gray-1)',
              color: 'var(--mantine-color-dark-6)',
              boxShadow: 'var(--mantine-shadow-md)',
            },
          }}
        >
          Methodology
        </Button>

      </header>

      {/* line break */}
        <hr className = {styles.greyLine}></hr>

    </div>
  )
}