import { Routes, Route } from "react-router-dom";
import { Home } from './pages/Home';
import { Dashboard } from './pages/Dashboard';
import { Toaster } from "sonner";


// root,  layout,  router
export default function App() {
  return(
    <>
      <Toaster position="top-right"/>
      <Routes>
        <Route path="/" element={<Home />} />
        <Route path="/dashboard/:cbsa_code/:occ_code" element={<Dashboard />} />
      </Routes>
    </>
  );
}