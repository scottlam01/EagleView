import { Routes, Route } from "react-router-dom";
import { Home } from './pages/Home';
import { Dashboard } from './pages/Dashboard';
import { Toaster } from "sonner";
import { Methodology } from './pages/Methodology';


// root,  layout,  router
export default function App() {
  return(
    <>
      <Toaster position="top-right"/>
      <Routes>
        <Route path="/" element={<Home />} />
        <Route path="/dashboard/:cbsa_code/:occ_code" element={<Dashboard />} />
        <Route path="/methodology" element={<Methodology />} />
      </Routes>
    </>
  );
}