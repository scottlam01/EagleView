import { useState, useRef, useEffect } from 'react';
import { useNavigate } from "react-router-dom";
import styles from './Searchbar.module.css';
import { toast } from "sonner";
import { AnimatePresence, motion } from "motion/react";

export default function Searchbar () {

  type Occupation = {
    occ_code: string;
    occ_title: string;
  };
  
  type Area = {
    cbsa_code: string;
    area_title: string;
  }

  const [isOccupationFocused, setIsOccupationFocused] = useState(false);
  const [isAreaFocused, setIsAreaFocused] = useState(false);

  const idleOccupations = [
    "Disc Jockeys, Except Radio",
    "Broadcast Announcers and Radio Disc Jockeys",
    "Customer Service Representatives",
    "Teaching Assistants, Except Postsecondary",
    "Inspectors, Testers, Sorters, Samplers, and Weighers",
    "Library Assistants, Clerical",
    "Etchers and Engravers",
    "Materials Engineers",
    "Funeral Home Managers",
    "Locksmiths and Safe Repairers",
    "Human Resources Specialists",
    "Operating Engineers and Other Construction Equipment Operators",
    "Dermatologists",
    "Actuaries",
    "Parts Salespersons",
    "Lawyers",
    "Judges, Magistrate Judges, and Magistrates",
    "Tile and Stone Setters",
    "Chemical Plant and System Operators",
    "Automotive and Watercraft Service Attendants",
    "General Internal Medicine Physicians",
    "Anesthesiologists",
    "Education Teachers, Postsecondary",
    "Production, Planning, and Expediting Clerks",
    "First-Line Supervisors of Security Workers",
    "Sailors and Marine Oilers",
    "Helpers--Electricians",
    "First-Line Supervisors of Personal Service Workers",
    "Receptionists and Information Clerks",
    "Wellhead Pumpers",
    "Arbitrators, Mediators, and Conciliators",
    "Building Cleaning Workers, All Other",
    "Materials Scientists",
    "Petroleum Pump System Operators, Refinery Operators, and Gaugers",
    "Insulation Workers, Floor, Ceiling, and Wall",
    "Microbiologists",
    "Nurse Anesthetists",
    "Child, Family, and School Social Workers",
    "Producers and Directors",
    "Family Medicine Physicians",
    "Security and Fire Alarm Systems Installers",
    "Environmental Scientists and Specialists, Including Health",
    "Butchers and Meat Cutters",
    "Helpers, Construction Trades, All Other",
    "Semiconductor Processing Technicians",
    "Multiple Machine Tool Setters, Operators, and Tenders, Metal and Plastic",
    "Genetic Counselors",
    "Tax Preparers",
    "Gambling Surveillance Officers and Gambling Investigators",
    "Teachers and Instructors, All Other",
    "Heavy and Tractor-Trailer Truck Drivers",
    "Bus Drivers, Transit and Intercity",
    "Painting, Coating, and Decorating Workers",
    "Advertising Sales Agents",
    "Graders and Sorters, Agricultural Products",
    "Patternmakers, Metal and Plastic",
    "Aircraft Structure, Surfaces, Rigging, and Systems Assemblers",
    "Law Teachers, Postsecondary",
    "Food Processing Workers, All Other",
    "Recreational Vehicle Service Technicians",
    "Public Relations Managers",
    "Animal Trainers",
    "Credit Analysts",
    "Healthcare Practitioners and Technical Workers, All Other",
    "Pressers, Textile, Garment, and Related Materials",
    "Geographers",
    "Hosts and Hostesses, Restaurant, Lounge, and Coffee Shop",
    "Plating Machine Setters, Operators, and Tenders, Metal and Plastic",
    "Sales Representatives, Wholesale and Manufacturing, Technical and Scientific Products",
    "Textile Cutting Machine Setters, Operators, and Tenders",
    "Radiation Therapists",
    "Human Resources Managers",
    "Physics Teachers, Postsecondary",
    "Skincare Specialists",
    "Diagnostic Medical Sonographers",
    "Athletic Trainers",
    "Boilermakers",
    "Special Effects Artists and Animators",
    "Food Servers, Nonrestaurant",
    "Office Machine Operators, Except Computer",
    "Merchandise Displayers and Window Trimmers",
    "Set and Exhibit Designers",
    "Electrical and Electronics Installers and Repairers, Transportation Equipment",
    "Pharmacy Aides",
    "Dishwashers",
    "Environmental Science and Protection Technicians, Including Health",
    "Mail Clerks and Mail Machine Operators, Except Postal Service",
    "Animal Breeders",
    "Helpers--Production Workers",
    "Food Science Technicians",
    "Pipelayers",
    "Software Quality Assurance Analysts and Testers",
    "School Bus Monitors",
    "Farmworkers, Farm, Ranch, and Aquacultural Animals",
    "Gambling Cage Workers",
    "Special Education Teachers, Middle School",
    "Barbers",
    "Licensed Practical and Licensed Vocational Nurses",
    "Paralegals and Legal Assistants",
    "Anthropologists and Archeologists"
  ]

  const idleAreas =[
    "Aguadilla-Isabela, PR",
    "San Jose-Sunnyvale-Santa Clara, CA",
    "New Orleans-Metairie, LA",
    "Rochester, MN",
    "Jackson, MI",
    "Terre Haute, IN",
    "Pittsfield, MA",
    "Dalton, GA",
    "Albuquerque, NM",
    "College Station-Bryan, TX",
    "Lake Charles, LA",
    "Evansville, IN-KY",
    "Montgomery, AL",
    "Mankato-North Mankato, MN",
    "Dover-Durham, NH-ME",
    "Gulfport-Biloxi-Pascagoula, MS",
    "Bloomsburg-Berwick, PA",
    "Charleston, WV",
    "Gainesville, FL",
    "Muskegon, MI",
    "Morgantown, WV",
    "Baton Rouge, LA",
    "Auburn-Opelika, AL",
    "Urban Honolulu, HI",
    "Odessa, TX",
    "Mansfield, OH",
    "Yuma, AZ",
    "Springfield, IL",
    "Milwaukee-Waukesha-West Allis, WI",
    "Roanoke, VA",
    "Columbia, SC",
    "Wheeling, WV-OH",
    "Chicago-Naperville-Elgin, IL-IN-WI",
    "Lebanon, PA",
    "Leominster-Gardner, MA",
    "Dallas-Fort Worth-Arlington, TX",
    "Morristown, TN",
    "Niles-Benton Harbor, MI",
    "Bridgeport-Stamford-Norwalk, CT",
    "Guayama, PR",
    "New Bedford, MA",
    "Corpus Christi, TX",
    "Worcester, MA-CT",
    "Bloomington, IL",
    "Anchorage, AK",
    "Oxnard-Thousand Oaks-Ventura, CA",
    "Allentown-Bethlehem-Easton, PA-NJ",
    "Gainesville, GA",
    "Burlington, NC",
    "Yakima, WA",
    "Lexington-Fayette, KY",
    "Danville, IL",
    "Florence, SC",
    "Sioux City, IA-NE-SD",
    "Grants Pass, OR",
    "Champaign-Urbana, IL",
    "Bismarck, ND",
    "New York-Newark-Jersey City, NY-NJ-PA",
    "Salisbury, MD-DE",
    "Tulsa, OK",
    "Lawrence, KS",
    "Macon, GA",
    "Santa Fe, NM",
    "Flagstaff, AZ",
    "Salem, OR",
    "Cleveland, TN",
    "Pittsburgh, PA",
    "Rockford, IL",
    "Bay City, MI",
    "Providence-Warwick, RI-MA",
    "Tucson, AZ",
    "Columbus, OH",
    "Lawton, OK",
    "Minneapolis-St. Paul-Bloomington, MN-WI",
    "St. Louis, MO-IL",
    "San Angelo, TX",
    "Birmingham-Hoover, AL",
    "Seattle-Tacoma-Bellevue, WA",
    "Sheboygan, WI",
    "Provo-Orem, UT",
    "Wenatchee, WA",
    "Atlanta-Sandy Springs-Roswell, GA",
    "Canton-Massillon, OH",
    "Owensboro, KY",
    "Mayaguez, PR",
    "Saginaw, MI",
    "Sacramento--Roseville--Arden-Arcade, CA",
    "Columbus, IN",
    "Indianapolis-Carmel-Anderson, IN",
    "Sioux Falls, SD",
    "Syracuse, NY",
    "Eau Claire, WI",
    "Tuscaloosa, AL",
    "Cape Girardeau, MO-IL",
    "Johnson City, TN",
    "York-Hanover, PA",
    "Greeley, CO",
    "Albany, GA",
    "Philadelphia-Camden-Wilmington, PA-NJ-DE-MD",
    "Rapid City, SD"
  ]

  const [idleOccupationIndex, setIdleOccupationIndex] = useState(0);
  const [idleAreaIndex, setIdleAreaIndex] = useState(0);

  // these flags indicate user is done selecting, so when they click the result, 
  // it doesn't cause another fetch since the intial query changed to the result
  const [isSelectingOccupation, setIsSelectingOccupation] = useState(false);
  const [isSelectingArea, setIsSelectingArea] = useState(false);

  const [queryOccupation, setQueryOccupation] = useState("");
  const [queryAreas, setQueryAreas] = useState("");

  const [occupationSuggestions, setOccupationSuggestions] = useState<Occupation[]>([]);
  const [areaSuggestions, setAreaSuggestions] = useState<Area[]>([]);

  const [selectedCbsaCode, setSelectedCbsaCode] = useState<string | null>(null);
  const [selectedOccCode, setSelectedOccCode] = useState<string | null>(null);

  const occupationSearchRef = useRef<HTMLDivElement>(null);
  const areaSearchRef = useRef<HTMLDivElement>(null);

  const navigate = useNavigate();

  // idle animation for occupation, cycles through idle occupations
  useEffect(() => {
    if (queryOccupation || isOccupationFocused) return;

    const timer = setInterval(() => {
      setIdleOccupationIndex((prev) =>
        (prev + 1) % idleOccupations.length
      );
    }, 3000);

    return () => clearInterval(timer);
  }, [queryOccupation, isOccupationFocused]);

  // idle animation for area, cycles through idle areas
  useEffect(() => {
    if (queryAreas || isAreaFocused) return;

    const timer = setInterval(() => {
      setIdleAreaIndex((prev) =>
        (prev + 1) % idleAreas.length
      );
    }, 3000);

    return () => clearInterval(timer);
  }, [queryAreas, isAreaFocused]);

  // fetch occupations as user types
  useEffect(() => {
    // if fetch is from user selecting a dropdown option, don't fetch
    if (isSelectingOccupation) {
      setIsSelectingOccupation(false);
      return;
    }

    const timer = setTimeout(() => {
      fetchOccupations(queryOccupation);
    }, 100);

    return () => clearTimeout(timer);
  }, [queryOccupation]);

  // fetch areas as user types
  useEffect(() => {

    // if fetch is from user selecting a dropdown option, don't fetch
    if (isSelectingArea) {
      setIsSelectingArea(false);
      return;
    }

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

 

// search button verifies valid cbsa and occ code then navigates to dashboard
const handleSearch = async () => {

  if (!selectedCbsaCode || !selectedOccCode) {
    toast.error("Please select a valid occupation and location from the dropdown.");
    return;
  }

  const response = await fetch(
  `http://localhost:8000/validate_search?cbsa_code=${selectedCbsaCode}&occ_code=${selectedOccCode}`
  );

  const data = await response.json();

  if (!data) {
    toast.error("No data available for this occupation and location. Select valid occupation and location from dropdown.");
    return;
  }

  navigate(`/dashboard/${selectedCbsaCode}/${selectedOccCode}`, {
    state: {
      occupation: queryOccupation,
      area: queryAreas,
    },
  });
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

        <div className={styles.inputContainer}>
          <AnimatePresence mode="wait">
            {!queryOccupation && !isOccupationFocused && (
              <motion.span
                key={idleOccupationIndex}
                className={styles.idleText}
                initial={{ y: 20, opacity: 0 }}
                animate={{ y: 0, opacity: 1 }}
                exit={{ y: -20, opacity: 0 }}
                transition={{ duration: 0.4 }}
              >
                {idleOccupations[idleOccupationIndex]}
              </motion.span>
            )}
          </AnimatePresence>

          <input
            type="text"
            value={queryOccupation}
            placeholder=""
            onChange={(e) => setQueryOccupation(e.target.value)}
            onFocus={() => {
              setIsOccupationFocused(true);
              fetchOccupations(queryOccupation);
            }}
            onBlur={() => setIsOccupationFocused(false)}
          />
        </div>
        
        {/* Only show the dropdown if there is at least one suggestion */}
        {occupationSuggestions.length > 0 && (
          <ul className={styles.dropdown}>
            {occupationSuggestions.map((occupation) => (
              <li
                key={occupation.occ_code}
                onMouseDown={() => {
                  setIsSelectingOccupation(true);
                  setQueryOccupation(occupation.occ_title);
                  setSelectedOccCode(occupation.occ_code);
                  console.log(selectedOccCode);
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

      <div className={styles.inputContainer}>
        <AnimatePresence mode="wait">
          {!queryAreas && !isAreaFocused && (
            <motion.span
              key={idleAreaIndex}
              className={styles.idleText}
              initial={{ y: 20, opacity: 0 }}
              animate={{ y: 0, opacity: 1 }}
              exit={{ y: -20, opacity: 0 }}
              transition={{ duration: 0.4 }}
            >
              {idleAreas[idleAreaIndex]}
            </motion.span>
          )}
        </AnimatePresence>

        <input
          type="text"
          value={queryAreas}
          placeholder=""
          onChange={(e) => setQueryAreas(e.target.value)}
          onFocus={() => {
            setIsAreaFocused(true);
            fetchAreas(queryAreas);
          }}
          onBlur={() => setIsAreaFocused(false)}
        />
      </div>


      {/* Only show the dropdown if there is at least one suggestion */}
        {areaSuggestions.length > 0 && (
          <ul className={styles.dropdown}>
            {areaSuggestions.map((areas) => (
              <li
                key={areas.cbsa_code}
                onMouseDown={() => {
                  setIsSelectingArea(true);
                  setQueryAreas(areas.area_title);
                  setSelectedCbsaCode(areas.cbsa_code);
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