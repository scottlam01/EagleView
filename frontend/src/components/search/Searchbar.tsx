import { useState, useRef, useEffect } from 'react';
import { useNavigate } from "react-router-dom";
import styles from './Searchbar.module.css';

export default function Searchbar () {

  type Occupation = {
    occ_code: string;
    occ_title: string;
  };
  
  type Area = {
    cbsa_code: string;
    area_title: string;
  }

  const [queryOccupation, setQueryOccupation] = useState("");
  const [queryAreas, setQueryAreas] = useState("");

  const [occupationSuggestions, setOccupationSuggestions] = useState<Occupation[]>([]);
  const [areaSuggestions, setAreaSuggestions] = useState<Area[]>([]);

  const occupationSearchRef = useRef<HTMLDivElement>(null);
  const areaSearchRef = useRef<HTMLDivElement>(null);

  const navigate = useNavigate();

  // fetch occupations as user types
  useEffect(() => {
    const timer = setTimeout(() => {
      fetchOccupations(queryOccupation);
    }, 100);

    return () => clearTimeout(timer);
  }, [queryOccupation]);

  // fetch areas as user types
  useEffect(() => {
    const timer = setTimeout(() => {
      fetchAreas(queryAreas);
    }, 100);

    return () => clearTimeout(timer);
  }, [queryAreas]);

  // Close occupation dropdown
  useEffect(() => {
    function handleOccupationClickOutside(event: MouseEvent) {
      if (
        occupationSearchRef.current &&
        !occupationSearchRef.current.contains(event.target as Node)
      ) {
        setOccupationSuggestions([]);
      }
    }

    document.addEventListener("mousedown", handleOccupationClickOutside);

    return () => {
      document.removeEventListener("mousedown", handleOccupationClickOutside);
    };
  }, []);

 

  // search button navigates to dashboard
  const handleSearch = () => {
    navigate("/dashboard");
  };


// Close area dropdown
useEffect(() => {
  function handleAreaClickOutside(event: MouseEvent) {
    if (
      areaSearchRef.current &&
      !areaSearchRef.current.contains(event.target as Node)
    ) {
      setAreaSuggestions([]);
    }
  }

  document.addEventListener("mousedown", handleAreaClickOutside);

  return () => {
    document.removeEventListener("mousedown", handleAreaClickOutside);
  };
}, []);

  // gets occupations from backend
  const fetchOccupations = async (query: string) => {
    if (!query.trim()) {
      setOccupationSuggestions([]);
      return;
    }
    try {
      const response = await fetch(
        `http://localhost:8000/occupations?q=${encodeURIComponent(query)}`
      );
      const data = await response.json();
      setOccupationSuggestions(data);
    } catch (error) {
      console.error(error);
    }
  };

  // gets cities from backend
  const fetchAreas = async (query: string) => {
    if (!query.trim()) {
      setAreaSuggestions([]);
      return;
    }
    try {
      const response = await fetch(
        `http://localhost:8000/areas?q=${encodeURIComponent(query)}`
      );
      const data = await response.json();
      setAreaSuggestions(data);
    } catch (error) {
      console.error(error);
    }
  }

  return ( 
    <div className = {styles.searchbarContainer}>
      <div tabIndex={-1} onBlur={() => setOccupationSuggestions([])} className = {styles.occupationSearchContainer}>

        <input
          type="text"
          value={queryOccupation}
          placeholder="enter occupation..."
          onChange={(e) => setQueryOccupation(e.target.value)}
          onFocus={() => fetchOccupations(queryOccupation)}
        />
        
        {/* Only show the dropdown if there is at least one suggestion */}
        {occupationSuggestions.length > 0 && (
          <ul className={styles.dropdown}>
            {occupationSuggestions.map((occupation) => (
              <li
                key={occupation.occ_code}
                onMouseDown={() => {
                  setQueryOccupation(occupation.occ_title);
                  setOccupationSuggestions([]);
                }}
              >
                {occupation.occ_title}
              </li>
            ))}
          </ul>
        )}

    </div>

    <div tabIndex={-1} onBlur={() => setAreaSuggestions([])}  className = {styles.areaSearchContainer}>
      <input
      type="text" 
      value={queryAreas} 
      placeholder='enter state or city...'
      onChange={(e) => setQueryAreas(e.target.value)}
      onFocus={() => fetchAreas(queryAreas)}> 
      </input>

      {/* Only show the dropdown if there is at least one suggestion */}
        {areaSuggestions.length > 0 && (
          <ul className={styles.dropdown}>
            {areaSuggestions.map((areas) => (
              <li
                key={areas.cbsa_code}
                onMouseDown={() => {
                  setQueryAreas(areas.area_title);
                  setAreaSuggestions([]);
                }}
              >
                {areas.area_title}
              </li>
            ))}
          </ul>
        )}

      
    </div>
    
    <button onClick={handleSearch}>
      :D
    </button>
    
    </div>
  )
}