import { useState } from 'react';
import { useEffect } from 'react';
import styles from './Searchbar.module.css';

export default function Searchbar () {

  const [queryOccupation, setQueryOccupation] = useState("");
  const [queryLocation, setQueryLocation] = useState("");

  return ( 
    <div className = {styles.searchbarContainer}>

      <input
      type="occupation" 
      value={queryOccupation} 
      onChange={(e) => setQueryOccupation(e.target.value)}> 
      </input>

      <input
      type="location" 
      value={queryLocation} 
      onChange={(e) => setQueryLocation(e.target.value)}> 
      </input>

      <button />

    </div>
  )
}