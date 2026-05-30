import { useState } from 'react';
import styles from './Searchbar.module.css';

export default function Searchbar () {

  const [queryOccupation, setQueryOccupation] = useState("");
  const [queryLocation, setQueryLocation] = useState("");

  return ( 
    <div className = {styles.searchBar}></div>
  )
}