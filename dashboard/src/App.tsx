import { Routes, Route } from 'react-router-dom'
import Layout from './components/Layout'
import Dashboard from './pages/Dashboard'
import Jobs from './pages/Jobs'
import History from './pages/History'
import Compare from './pages/Compare'
import Agents from './pages/Agents'
import System from './pages/System'

function App() {
  return (
    <Layout>
      <Routes>
        <Route path="/" element={<Dashboard />} />
        <Route path="/jobs" element={<Jobs />} />
        <Route path="/history" element={<History />} />
        <Route path="/compare" element={<Compare />} />
        <Route path="/agents" element={<Agents />} />
        <Route path="/system" element={<System />} />
      </Routes>
    </Layout>
  )
}

export default App
